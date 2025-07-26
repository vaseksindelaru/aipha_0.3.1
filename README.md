# Proyecto Aipha

Laboratorio de IA y trading cuantitativo para el proyecto Aipha.

## Descripción

Este proyecto es un entorno de desarrollo completo para investigar, desarrollar y ejecutar estrategias de trading algorítmico basadas en inteligencia artificial.

## Stack Tecnológico

*   **Orquestación de servicios:** Docker & Docker Compose
*   **Lenguaje principal:** Python
*   **Gestión de entorno Python:** pyenv + Poetry
*   **Base de datos relacional:** PostgreSQL
*   **Caché en memoria:** Redis

## Cómo Empezar

1.  Asegurarse de tener Docker, pyenv y Poetry instalados.
2.  Levantar los servicios de datos: `docker compose up -d`
3.  Instalar las dependencias de Python: `poetry install`
4.  Activar el entorno virtual: `poetry shell`
