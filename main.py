from fastapi import FastAPI, HTTPException, Depends, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, validator
from google.cloud import storage
from google.oauth2 import service_account
from datetime import timedelta
import instaloader
import aiohttp
import asyncio
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
import os
from dotenv import load_dotenv
import logging
from typing import Optional, List, Dict
import re
from io import BytesIO
import json
from contextlib import asynccontextmanager

# Configurar logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Cargar variables de entorno
load_dotenv()

# Configurar rate limiter
limiter = Limiter(key_func=get_remote_address)

class VideoRequest(BaseModel):
    url: str

    @validator('url')
    def validate_instagram_url(cls, v):
        if not re.match(r'https?:\/\/(www\.)?instagram\.com\/(p|reel)\/[\w-]+', v):
            raise ValueError("URL de Instagram no válida. Debe ser un enlace a un post o reel de Instagram")
        return v

class InstaloaderPool:
    def __init__(self, pool_size=3):
        self.pool = []
        self.lock = asyncio.Lock()
        self.current_index = 0
        
        for _ in range(pool_size):
            loader = self._create_instaloader()
            if loader:
                self.pool.append(loader)
        
        if not self.pool:
            raise RuntimeError("No se pudo crear ninguna instancia de Instaloader")

    def _create_instaloader(self):
        try:
            loader = instaloader.Instaloader(
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
            loader.context._session.headers['User-Agent'] = (
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                '(KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            )
            
            instagram_username = os.getenv('INSTAGRAM_USERNAME')
            instagram_password = os.getenv('INSTAGRAM_PASSWORD')
            
            if instagram_username and instagram_password:
                try:
                    loader.login(instagram_username, instagram_password)
                    logger.info(f"Login exitoso en Instagram como {instagram_username}")
                except Exception as e:
                    logger.error(f"Error en login: {str(e)}")
                    # Continuar sin login si falla
            
            return loader
            
        except Exception as e:
            logger.error(f"Error al crear Instaloader: {str(e)}")
            return None

    async def get_loader(self):
        async with self.lock:
            loader = self.pool[self.current_index]
            self.current_index = (self.current_index + 1) % len(self.pool)
            return loader

class InstagramHandler:
    def __init__(self, loader: instaloader.Instaloader):
        self.L = loader
        self._session = aiohttp.ClientSession()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._session.close()

    async def get_post_from_url(self, url: str) -> tuple[Optional[instaloader.Post], list]:
        try:
            match = re.search(r'instagram\.com/(?:p|reel)/([A-Za-z0-9_-]+)', url)
            if not match:
                raise ValueError("URL de Instagram no válida")
            
            shortcode = match.group(1)
            logger.info(f"Procesando post con shortcode: {shortcode}")
            
            # Obtener el post (operación síncrona de Instaloader)
            post = await asyncio.to_thread(
                instaloader.Post.from_shortcode,
                self.L.context,
                shortcode
            )
            
            if not post:
                raise HTTPException(status_code=404, detail="Post no encontrado")

            video_urls = []

            if post.typename == 'GraphSidecar':
                logger.info("Procesando carrusel de medios")
                nodes = await asyncio.to_thread(post.get_sidecar_nodes)
                for node in nodes:
                    if node.is_video:
                        video_urls.append({
                            'url': node.video_url,
                            'thumbnail': node.display_url
                        })
            elif post.is_video:
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

    async def download_video_to_memory(self, video_url: str) -> BytesIO:
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            
            async with self._session.get(video_url, headers=headers) as response:
                response.raise_for_status()
                video_data = await response.read()
                return BytesIO(video_data)

        except Exception as e:
            logger.error(f"Error al descargar video: {str(e)}")
            raise

class GCSHandler:
    def __init__(self):
        credentials_path = os.getenv('GCS_CREDENTIALS_PATH')
        if not os.path.exists(credentials_path):
            raise FileNotFoundError(f"Archivo de credenciales no encontrado en: {credentials_path}")

        credentials = service_account.Credentials.from_service_account_file(
            credentials_path,
            scopes=['https://www.googleapis.com/auth/cloud-platform']
        )

        self.client = storage.Client(credentials=credentials)
        self.bucket_name = os.getenv('GCS_BUCKET_NAME')
        self.bucket = self.client.bucket(self.bucket_name)
        logger.info(f"GCS Handler inicializado para bucket: {self.bucket_name}")

    async def upload_from_memory(self, video_buffer: BytesIO, filename: str) -> str:
        try:
            blob = self.bucket.blob(filename)
            # Convertir la operación síncrona a asíncrona
            await asyncio.to_thread(
                blob.upload_from_file,
                video_buffer,
                content_type='video/mp4'
            )
            
            url = blob.generate_signed_url(
                version="v4",
                expiration=timedelta(minutes=15),
                method="GET"
            )
            return url
        except Exception as e:
            logger.error(f"Error al subir archivo a GCS: {str(e)}")
            raise

# Configuración de la aplicación
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Inicialización
    app.state.instaloader_pool = InstaloaderPool()
    app.state.gcs_handler = GCSHandler()
    yield
    # Limpieza
    app.state.instaloader_pool = None
    app.state.gcs_handler = None

app = FastAPI(title="Instagram Video Download API", lifespan=lifespan)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# Configurar CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

async def get_instagram_handler():
    loader = await app.state.instaloader_pool.get_loader()
    async with InstagramHandler(loader) as handler:
        yield handler

@app.post("/download-instagram-video")
@limiter.limit("20/minute")
async def download_instagram_video(
    request: Request,
    video_request: VideoRequest,
    instagram_handler: InstagramHandler = Depends(get_instagram_handler)
):
    try:
        logger.info(f"Procesando solicitud para URL: {video_request.url}")
        
        if app.state.gcs_handler is None:
            raise HTTPException(status_code=500, detail="Servicio de almacenamiento no inicializado")

        # Obtener el post y los videos disponibles
        post, video_urls = await instagram_handler.get_post_from_url(video_request.url)
        
        # Lista para almacenar las URLs de descarga
        download_urls = []

        # Procesar cada video encontrado
        for idx, video_info in enumerate(video_urls):
            video_buffer = await instagram_handler.download_video_to_memory(video_info['url'])
            
            video_filename = f"instagram_video_{post.shortcode}_{idx}_{os.urandom(4).hex()}.mp4"
            
            signed_url = await app.state.gcs_handler.upload_from_memory(video_buffer, video_filename)
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
            "message": f"Se encontraron {len(download_urls)} videos en la publicación",
            "rate_limit_info": {
                "remaining": request.headers.get("X-RateLimit-Remaining", "N/A"),
                "reset": request.headers.get("X-RateLimit-Reset", "N/A")
            }
        }
        
        return response_data

    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error inesperado: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error interno del servidor: {str(e)}")

@app.get("/active-page")
async def health_check():
    return {
        "status": "healthy",
        "version": "1.0.0"
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
