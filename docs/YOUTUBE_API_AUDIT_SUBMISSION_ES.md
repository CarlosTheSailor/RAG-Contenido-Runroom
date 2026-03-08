# Documento de Soporte para Auditoría de YouTube Data API

Fecha: 8 de marzo de 2026  
Proyecto Google Cloud: `noted-function-94615`  
Nombre del servicio interno: `RAG-Contenido-Runroom / Phase 0 Preview`

## 1. Resumen ejecutivo
Este proyecto usa YouTube Data API v3 exclusivamente en modo de lectura para obtener la descripción actual de un video del podcast Realworld y generar una propuesta editorial mejorada de forma local.

El caso de uso es interno y manual. No existe publicación automática ni actualización de contenido en YouTube.

## 2. Cliente de API y disponibilidad
Cliente único: herramienta CLI interna en entorno privado de Runroom.

- Tipo de cliente: Backend/CLI (sin frontend público)
- Acceso público: No
- Usuarios finales: Equipo interno de contenidos/marketing
- Frecuencia de uso: baja, bajo demanda
- Alcance actual: un vídeo por ejecución

Si no es posible acceso público al cliente, se adjunta screencast funcional completo (ver sección 10).

## 3. Endpoints y métodos usados
API usada: YouTube Data API v3.

Método usado actualmente:
- `videos.list`

Parámetros:
- `part=snippet`
- `id=<video_id>`

Campos de respuesta consumidos:
- `snippet.title`
- `snippet.description`
- `snippet.channelTitle`

No se usan endpoints de escritura (`videos.update`, `playlistItems.insert`, etc.).

## 4. Flujo funcional de la integración
1. El usuario ejecuta un comando interno de preview con identificador de episodio y URL de YouTube.
2. El sistema extrae `video_id` de la URL de YouTube.
3. El sistema llama `videos.list(part=snippet,id=<video_id>)`.
4. El sistema toma la descripción actual y la usa como “current description” para comparación.
5. El sistema genera una propuesta mejorada offline y un diff local.
6. Se guardan artefactos locales:
- `output/<episode_slug>/proposed_description.md`
- `output/<episode_slug>/qa_report.json`
- `output/<episode_slug>/diff.md`

## 5. Datos de YouTube tratados
Datos recuperados:
- Título del vídeo
- Descripción del vídeo
- Nombre del canal

No se recuperan:
- Datos privados de usuario
- Comentarios
- Historial de visualización
- Información de cuentas personales

## 6. Retención y almacenamiento
La información recuperada se usa para el proceso de comparación y QA en entorno interno.

- Persistencia principal: archivos locales del preview en el workspace interno
- Finalidad: revisión editorial antes de publicar cambios manuales
- No hay exposición pública de los datos vía API propia
- No hay venta ni cesión de datos a terceros

## 7. Seguridad y control de acceso
Medidas aplicadas:
- API key almacenada en variable de entorno (`YOUTUBE_API_KEY`)
- Restricción de clave a YouTube Data API v3
- Uso desde backend/CLI interno (no embebida en frontend público)
- Logging sin exponer la clave completa (solo estado, prefijo y longitud para diagnóstico)

Medidas recomendadas adicionales:
- Restringir key por IP de salida fija (si aplica)
- Rotación periódica de key
- Monitorización de cuotas y alertas

## 8. Cumplimiento de políticas
Este servicio cumple el alcance de lectura para uso editorial interno:
- Uso mínimo necesario para el caso de negocio
- Sin automatización de publicación en YouTube
- Sin modificación de metadatos de YouTube vía API
- Sin suplantación de cliente oficial
- Sin scraping fuera de API oficial

## 9. Justificación de cuota
Necesitamos cuota para llamadas de lectura de metadatos por vídeo en procesos internos de revisión editorial.

Uso estimado:
- Bajo volumen
- Flujo manual
- 1 llamada principal por vídeo en preview (más reintentos puntuales)

No se realizan operaciones masivas de alto consumo.

## 10. Evidencia operativa (sin screencast en esta solicitud)
Actualmente no se adjunta screencast porque el cliente no es público y está en fase interna.

En su lugar, se aporta evidencia técnica equivalente:
1. Flujo funcional detallado del cliente interno.
2. Endpoints y parámetros exactos usados (`videos.list`, `part=snippet`, `id=<video_id>`).
3. Artefactos de salida y trazabilidad local (`proposed_description.md`, `qa_report.json`, `diff.md`).
4. Confirmación explícita de alcance read-only actual (sin `videos.update` ni publicación automática).
5. Medidas de seguridad de clave API y restricciones recomendadas.

Si el equipo revisor lo considera estrictamente necesario, podemos aportar un screencast en una iteración posterior.

## 11. Comando real usado en el cliente interno
```bash
python3 -m src.cli preview-youtube-description \
  --episode r085 \
  --youtube-url "https://youtu.be/npyyu7T3PwM?si=YRl2pfu8az9PAkjz"
```

## 12. Contacto técnico
Responsable técnico: `Carlos Iglesias`  
Empresa: Runroom  
Correo: `carlos@runroom.com`

## 13. Declaración final
Declaramos que el uso actual de YouTube Data API v3 en este proyecto es de solo lectura, con finalidad editorial interna, sin automatización de publicación ni cambios de contenido en YouTube, y con controles para minimizar datos y proteger credenciales.
