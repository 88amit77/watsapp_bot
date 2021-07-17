from django.urls import path, include
from rest_framework import routers
from .app.views import (
    whatsappBot,
    paytmPaymentDifference,
    amazonPaymentDifference,
    flipkartPaymentDifference
)

from django.contrib.staticfiles.urls import staticfiles_urlpatterns


router = routers.DefaultRouter()

urlpatterns = [
    path('', include(router.urls)),
    path('whatapp_app/', whatsappBot.as_view(), name='whatapp_app'),
    path('paytm_payment_difference/', paytmPaymentDifference.as_view(), name='paytm_payment_difference_app'),
    path('amazon_payment_difference/', amazonPaymentDifference.as_view(), name='amazon_payment_difference_app'),
    path('flipkart_payment_difference/', flipkartPaymentDifference.as_view(), name='flipkart_payment_difference_app'),
]

urlpatterns += staticfiles_urlpatterns()

