from fastapi import FastAPI, HTTPException, BackgroundTasks, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, validator
from google.cloud import storage
from google.oauth2 import service_account
from datetime import timedelta
from contextlib import asynccontextmanager
import instaloader
import requests
import os
from dotenv import load_dotenv
import logging
from typing import Optional, AsyncGenerator
import re
from io import BytesIO
import json
import asyncio
from concurrent.futures import ThreadPoolExecutor

# Configurar logging más detallado
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Cargar variables de entorno
load_dotenv()

app = FastAPI(title="Instagram Video Download API")

# Configurar CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class VideoRequest(BaseModel):
    url: str

    @validator('url')
    def validate_instagram_url(cls, v):
        if not re.match(r'https?:\/\/(www\.)?instagram\.com\/(p|reel)\/[\w-]+', v):
            raise ValueError("URL de Instagram no válida. Debe ser un enlace a un post o reel de Instagram")
        return v

class InstaloaderPool:
    def __init__(self, pool_size: int = 5):
        self.pool_size = pool_size
        self._semaphore = asyncio.Semaphore(pool_size)
        self._pool: list[instaloader.Instaloader] = []
        self._lock = asyncio.Lock()
        
    async def initialize(self):
        async with self._lock:
            if not self._pool:
                for _ in range(self.pool_size):
                    loader = self._create_loader()
                    self._pool.append(loader)
                logger.info(f"Pool inicializado con {self.pool_size} instancias")

    def _create_loader(self) -> instaloader.Instaloader:
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
        loader.context._session.headers['User-Agent'] = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        
        # Login si hay credenciales
        instagram_username = os.getenv('INSTAGRAM_USERNAME')
        instagram_password = os.getenv('INSTAGRAM_PASSWORD')
        if instagram_username and instagram_password:
            try:
                loader.login(instagram_username, instagram_password)
                logger.info(f"Login exitoso en Instagram como {instagram_username}")
            except Exception as e:
                logger.error(f"Error en login: {str(e)}")
        
        return loader

    @asynccontextmanager
    async def get_loader(self) -> AsyncGenerator[instaloader.Instaloader, None]:
        async with self._semaphore:
            async with self._lock:
                loader = self._pool.pop()
            try:
                yield loader
            finally:
                async with self._lock:
                    self._pool.append(loader)

class InstagramHandler:
    def __init__(self, pool: InstaloaderPool):
        self.pool = pool
        self.executor = ThreadPoolExecutor(max_workers=10)

    async def get_post_from_url(self, url: str) -> tuple[Optional[instaloader.Post], list]:
        async with self.pool.get_loader() as loader:
            return await asyncio.get_event_loop().run_in_executor(
                self.executor,
                self._get_post_from_url_sync,
                loader,
                url
            )

    def _get_post_from_url_sync(self, loader: instaloader.Instaloader, url: str) -> tuple[Optional[instaloader.Post], list]:
        try:
            match = re.search(r'instagram\.com/(?:p|reel)/([A-Za-z0-9_-]+)', url)
            if not match:
                raise ValueError("URL de Instagram no válida")
            
            shortcode = match.group(1)
            logger.info(f"Procesando post con shortcode: {shortcode}")
            
            post = instaloader.Post.from_shortcode(loader.context, shortcode)
            
            if not post:
                raise HTTPException(status_code=404, detail="Post no encontrado")

            video_urls = []

            if post.typename == 'GraphSidecar':
                logger.info("Procesando carrusel de medios")
                for node in post.get_sidecar_nodes():
                    if node.is_video:
                        video_urls.append({
                            'url': node.video_url,
                            'thumbnail': node.display_url
                        })
            elif post.is_video:
                video_urls.append({
                    'url': post.video_url,
                    'thumbnail': post.url
                })

            if not video_urls:
                raise HTTPException(status_code=400, detail="No se encontraron videos en la publicación")

            return post, video_urls

        except Exception as e:
            logger.error(f"Error al procesar post: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))

    async def download_video_to_memory(self, video_url: str) -> BytesIO:
        return await asyncio.get_event_loop().run_in_executor(
            self.executor,
            self._download_video_to_memory_sync,
            video_url
        )

    def _download_video_to_memory_sync(self, video_url: str) -> BytesIO:
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
            credentials_json = os.getenv('GCS_CREDENTIALS_JSON')
            if credentials_json:
                try:
                    credentials_info = json.loads(credentials_json)
                    credentials = service_account.Credentials.from_service_account_info(
                        credentials_info,
                        scopes=['https://www.googleapis.com/auth/cloud-platform']
                    )
                    return storage.Client(credentials=credentials)
                except json.JSONDecodeError as e:
                    logger.error(f"Error decodificando JSON de credenciales: {str(e)}")
                    raise

            credentials_path = os.getenv('GCS_CREDENTIALS_PATH')
            if not credentials_path:
                raise ValueError("No se encontraron credenciales de GCS")

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
        self.executor = ThreadPoolExecutor(max_workers=5)
        logger.info(f"GCS Handler inicializado para bucket: {self.bucket_name}")

    async def upload_from_memory(self, video_buffer: BytesIO, filename: str) -> str:
        return await asyncio.get_event_loop().run_in_executor(
            self.executor,
            self._upload_from_memory_sync,
            video_buffer,
            filename
        )

    def _upload_from_memory_sync(self, video_buffer: BytesIO, filename: str) -> str:
        try:
            blob = self.bucket.blob(filename)
            blob.upload_from_file(video_buffer, content_type='video/mp4')
            
            url = blob.generate_signed_url(
                version="v4",
                expiration=timedelta(minutes=15),
                method="GET"
            )
            return url
        except Exception as e:
            logger.error(f"Error al subir archivo a GCS: {str(e)}")
            raise

# Variables globales para los handlers
loader_pool = InstaloaderPool()
instagram_handler = None
gcs_handler = None

@app.on_event("startup")
async def startup_event():
    global instagram_handler, gcs_handler
    try:
        # Inicializar el pool de Instaloader
        await loader_pool.initialize()
        
        # Inicializar handlers
        instagram_handler = InstagramHandler(loader_pool)
        gcs_handler = GCSHandler()
        
        logger.info("Todos los handlers inicializados correctamente")
    except Exception as e:
        logger.error(f"Error durante la inicialización: {str(e)}")
        raise

@app.on_event("shutdown")
async def shutdown_event():
    # Limpieza de recursos
    if hasattr(instagram_handler, 'executor'):
        instagram_handler.executor.shutdown(wait=True)
    if hasattr(gcs_handler, 'executor'):
        gcs_handler.executor.shutdown(wait=True)

async def cleanup_temp_files(temp_files: list):
    """Limpia archivos temporales en background"""
    for file in temp_files:
        try:
            if os.path.exists(file):
                os.remove(file)
        except Exception as e:
            logger.error(f"Error limpiando archivo temporal {file}: {str(e)}")

@app.post("/download-instagram-video")
async def download_instagram_video(
    request: VideoRequest,
    background_tasks: BackgroundTasks
):
    try:
        if not instagram_handler or not gcs_handler:
            raise HTTPException(status_code=500, detail="Servicios no inicializados")

        # Obtener el post y los videos disponibles
        post, video_urls = await instagram_handler.get_post_from_url(request.url)
        
        # Lista para almacenar las URLs de descarga y archivos temporales
        download_urls = []
        temp_files = []

        # Procesar cada video encontrado
        for idx, video_info in enumerate(video_urls):
            video_buffer = await instagram_handler.download_video_to_memory(video_info['url'])
            
            video_filename = f"instagram_video_{post.shortcode}_{idx}_{os.urandom(4).hex()}.mp4"
            
            # Subir a GCS y obtener URL firmada
            signed_url = await gcs_handler.upload_from_memory(video_buffer, video_filename)
            download_urls.append({
                'download_url': signed_url,
                'thumbnail': video_info['thumbnail']
            })
            
            # Agregar archivo a la lista de temporales si es necesario
            if hasattr(video_buffer, 'name'):
                temp_files.append(video_buffer.name)

        # Programar limpieza de archivos temporales
        if temp_files:
            background_tasks.add_task(cleanup_temp_files, temp_files)

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

@app.get("/active-page")
async def health_check():
    """Endpoint para verificar el estado del servicio"""
    return {
        "status": "healthy",
        "instaloader_pool_size": loader_pool.pool_size if loader_pool else 0
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)