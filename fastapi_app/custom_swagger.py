from fastapi.openapi.docs import get_swagger_ui_html
from fastapi import FastAPI
from starlette.responses import HTMLResponse

def add_custom_swagger_ui(app: FastAPI) -> None:
    """
    Add a custom Swagger UI route to ensure it loads correctly
    """
    @app.get("/custom-docs", include_in_schema=False)
    async def custom_swagger_ui_html():
        return get_swagger_ui_html(
            openapi_url=app.openapi_url,
            title=app.title + " - Swagger UI",
            oauth2_redirect_url=app.swagger_ui_oauth2_redirect_url,
            swagger_js_url="https://cdn.jsdelivr.net/npm/swagger-ui-dist@4.15.5/swagger-ui-bundle.js",
            swagger_css_url="https://cdn.jsdelivr.net/npm/swagger-ui-dist@4.15.5/swagger-ui.css",
        ) 