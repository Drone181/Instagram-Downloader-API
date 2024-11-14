from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, validator
from google.cloud import storage
from google.oauth2 import service_account
from datetime import timedelta
import instaloader
import requests
import os
from dotenv import load_dotenv
import logging
from typing import Optional
import re
from io import BytesIO
import json

# Configurar logging más detallado
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Cargar variables de entorno
load_dotenv()

app = FastAPI(title="Instagram Video Download API")

class VideoRequest(BaseModel):
    url: str

    @validator('url')
    def validate_instagram_url(cls, v):
        if not re.match(r'https?:\/\/(www\.)?instagram\.com\/(p|reel)\/[\w-]+', v):
            raise ValueError("URL de Instagram no válida. Debe ser un enlace a un post o reel de Instagram")
        return v

class InstagramHandler:
    def __init__(self):
        try:
            self.L = instaloader.Instaloader(
                download_videos=True,
                download_video_thumbnails=False,
                download_geotags=False,
                download_comments=False,
                save_metadata=False,
                compress_json=False,
                post_metadata_txt_pattern='',
                max_connection_attempts=3
            )
            # Configurar session
            self.L.context._session.headers['User-Agent'] = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            
            instagram_username = os.getenv('INSTAGRAM_USERNAME')
            instagram_password = os.getenv('INSTAGRAM_PASSWORD')
            if instagram_username and instagram_password:
                try:
                    self.L.login(instagram_username, instagram_password)
                    logger.info(f"Login exitoso en Instagram como {instagram_username}")
                except Exception as e:
                    logger.error(f"Error en login: {str(e)}")
                    raise
        except Exception as e:
            logger.error(f"Error al inicializar Instaloader: {str(e)}")
            raise
        
    def get_post_from_url(self, url: str) -> tuple[Optional[instaloader.Post], list]:
        """
        Obtiene el post y lista de videos disponibles
        Retorna una tupla con el post y una lista de URLs de videos
        """
        try:
            # Extraer shortcode
            match = re.search(r'instagram\.com/(?:p|reel)/([A-Za-z0-9_-]+)', url)
            if not match:
                raise ValueError("URL de Instagram no válida")
            
            shortcode = match.group(1)
            logger.info(f"Procesando post con shortcode: {shortcode}")
            
            # Obtener el post
            post = instaloader.Post.from_shortcode(self.L.context, shortcode)
            
            if not post:
                raise HTTPException(status_code=404, detail="Post no encontrado")

            logger.info(f"Tipo de post - Es carrusel: {post.typename == 'GraphSidecar'}")
            logger.info(f"Media count: {post.mediacount}")

            # Lista para almacenar URLs de videos
            video_urls = []

            # Manejar diferentes tipos de posts
            if post.typename == 'GraphSidecar':
                # Es un carrusel/álbum
                logger.info("Procesando carrusel de medios")
                for node in post.get_sidecar_nodes():
                    if node.is_video:
                        logger.info(f"Video encontrado en carrusel: {node.video_url}")
                        video_urls.append({
                            'url': node.video_url,
                            'thumbnail': node.display_url
                        })
            elif post.is_video:
                # Es un video único
                logger.info(f"Video único encontrado: {post.video_url}")
                video_urls.append({
                    'url': post.video_url,
                    'thumbnail': post.url
                })

            if not video_urls:
                raise HTTPException(
                    status_code=400, 
                    detail="No se encontraron videos en la publicación"
                )

            return post, video_urls

        except Exception as e:
            logger.error(f"Error al procesar post: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))

    def download_video_to_memory(self, video_url: str) -> BytesIO:
        """Descarga el video directamente a memoria"""
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            
            response = requests.get(video_url, headers=headers, stream=True, timeout=30)
            response.raise_for_status()
            
            video_buffer = BytesIO()
            for chunk in response.iter_content(chunk_size=8192):
                video_buffer.write(chunk)
            
            video_buffer.seek(0)
            return video_buffer

        except Exception as e:
            logger.error(f"Error al descargar video: {str(e)}")
            raise

class GCSConfig:
    @staticmethod
    def initialize_storage_client():
        try:
            credentials_path = os.getenv('GCS_CREDENTIALS_PATH')
            if not os.path.exists(credentials_path):
                raise FileNotFoundError(f"Archivo de credenciales no encontrado en: {credentials_path}")

            credentials = service_account.Credentials.from_service_account_file(
                credentials_path,
                scopes=['https://www.googleapis.com/auth/cloud-platform']
            )

            return storage.Client(credentials=credentials)
        except Exception as e:
            logger.error(f"Error inicializando el cliente de GCS: {str(e)}")
            raise

class GCSHandler:
    def __init__(self):
        self.client = GCSConfig.initialize_storage_client()
        self.bucket_name = os.getenv('GCS_BUCKET_NAME')
        self.bucket = self.client.bucket(self.bucket_name)
        logger.info(f"GCS Handler inicializado para bucket: {self.bucket_name}")

    def upload_from_memory(self, video_buffer: BytesIO, filename: str) -> str:
        """Sube un archivo desde memoria a GCS y retorna una URL firmada"""
        try:
            blob = self.bucket.blob(filename)
            blob.upload_from_file(video_buffer, content_type='video/mp4')
            
            # Generar URL firmada
            url = blob.generate_signed_url(
                version="v4",
                expiration=timedelta(minutes=15),
                method="GET"
            )
            return url
        except Exception as e:
            logger.error(f"Error al subir archivo a GCS: {str(e)}")
            raise

# Inicializar handlers
instagram_handler = InstagramHandler()
gcs_handler = None

try:
    gcs_handler = GCSHandler()
except Exception as e:
    logger.error(f"Error al inicializar GCS Handler: {str(e)}")

@app.on_event("startup")
async def startup_event():
    global gcs_handler
    if gcs_handler is None:
        try:
            gcs_handler = GCSHandler()
            logger.info("GCS Handler inicializado correctamente durante el startup")
        except Exception as e:
            logger.error(f"Error al inicializar GCS Handler durante startup: {str(e)}")
            raise

@app.post("/download-instagram-video")
async def download_instagram_video(request: VideoRequest):
    try:
        logger.info(f"Procesando solicitud para URL: {request.url}")
        
        if gcs_handler is None:
            raise HTTPException(status_code=500, detail="Servicio de almacenamiento no inicializado")

        # Obtener el post y los videos disponibles
        post, video_urls = instagram_handler.get_post_from_url(request.url)
        
        # Lista para almacenar las URLs de descarga
        download_urls = []

        # Procesar cada video encontrado
        for idx, video_info in enumerate(video_urls):
            video_buffer = instagram_handler.download_video_to_memory(video_info['url'])
            
            # Generar nombre único para cada video
            video_filename = f"instagram_video_{post.shortcode}_{idx}_{os.urandom(4).hex()}.mp4"
            
            # Subir a GCS y obtener URL firmada
            signed_url = gcs_handler.upload_from_memory(video_buffer, video_filename)
            download_urls.append({
                'download_url': signed_url,
                'thumbnail': video_info['thumbnail']
            })

        response_data = {
            "success": True,
            "videos": download_urls,
            "post_info": {
                "caption": post.caption if post.caption else "Sin descripción",
                "likes": post.likes,
                "date": post.date_local.isoformat(),
                "media_count": post.mediacount
            },
            "message": f"Se encontraron {len(download_urls)} videos en la publicación"
        }
        
        return response_data

    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error inesperado: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error interno del servidor: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)