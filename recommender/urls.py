from django.urls import path
from .views import get_recommendations_api

urlpatterns = [
    path('recommendations/<int:visitor_id>/', get_recommendations_api, name='get_recommendations'),
]
