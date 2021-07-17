import os
from datetime import timedelta

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

SECRET_KEY = 'qwertyui1234567sdfghj'

DEBUG = True

ALLOWED_HOSTS = ['*']

INSTALLED_APPS = [
	'django.contrib.contenttypes',
	'django.contrib.auth',
	'django.contrib.staticfiles',
	'rest_framework',
	'drf_yasg',
	'api',
	'djoser',
	'corsheaders',
	'storages'
]

MIDDLEWARE = [
	'django.middleware.security.SecurityMiddleware',
	'corsheaders.middleware.CorsMiddleware',
	'django.middleware.common.CommonMiddleware',
	'django.middleware.clickjacking.XFrameOptionsMiddleware',
]

ROOT_URLCONF = 'api.urls'

WSGI_APPLICATION = 'api.wsgi.application'

DATABASES = {
	'default': {
		'ENGINE': 'django.db.backends.postgresql_psycopg2',
		'NAME': 'testers',
		'USER': 'postgres',
		'PASSWORD': 'buymore3',
		'HOST': 'buymore-2.cluster-cegnfd8ehfoc.ap-south-1.rds.amazonaws.com',
		'PORT': '',
	}
}
#DATABASES = {
#'default': {
#'ENGINE': 'django.db.backends.sqlite3',
#'NAME': BASE_DIR / 'db.sqlite3',
#}
#}


LANGUAGE_CODE = 'en-us'

TIME_ZONE = 'UTC'

USE_I18N = True

USE_L10N = True

USE_TZ = True

REST_FRAMEWORK = {
	'UNAUTHENTICATED_USER': None,
	'DEFAULT_AUTHENTICATION_CLASSES': [
		'rest_framework_simplejwt.authentication.JWTAuthentication',
	],
	'DEFAULT_SCHEMA_CLASS': 'rest_framework.schemas.coreapi.AutoSchema'
}
TEMPLATES = [
	{
		'BACKEND': 'django.template.backends.django.DjangoTemplates',
		'DIRS': [],
		'APP_DIRS': True,
		'OPTIONS': {
			'context_processors': [
				'django.template.context_processors.debug',
				'django.template.context_processors.request',
				'django.contrib.auth.context_processors.auth',
				'django.contrib.messages.context_processors.messages',
			],
		},
	},
]

STATIC_URL = '/app/static/'

STATIC_ROOT = os.path.join(BASE_DIR, "static")

CORS_ORIGIN_ALLOW_ALL = True
