# init-ollama.ps1
Write-Host "🚀 Inicialización de Ollama" -ForegroundColor Green

# Paso 1: Verificar que Docker esté corriendo
Write-Host "1. Verificando Docker..." -ForegroundColor Yellow
try {
    docker version | Out-Null
    Write-Host "   ✅ Docker está funcionando" -ForegroundColor Green
}
catch {
    Write-Host "   ❌ Docker no está funcionando. Inicia Docker Desktop primero." -ForegroundColor Red
    exit 1
}

# Paso 2: Iniciar Ollama
Write-Host "2. Iniciando contenedor de Ollama..." -ForegroundColor Yellow
docker-compose up ollama -d

# Paso 3: Esperar a que Ollama esté listo
Write-Host "3. Esperando a que Ollama esté listo..." -ForegroundColor Yellow
$maxRetries = 30
$retryCount = 0

do {
    try {
        $response = Invoke-RestMethod -Uri "http://localhost:11434/api/tags" -TimeoutSec 10
        Write-Host "   ✅ Ollama está listo" -ForegroundColor Green
        break
    }
    catch {
        $retryCount++
        Write-Host "   🔄 Intento $retryCount/$maxRetries - Esperando..." -ForegroundColor Gray
        Start-Sleep -Seconds 5
    }
} while ($retryCount -lt $maxRetries)

if ($retryCount -eq $maxRetries) {
    Write-Host "   ❌ Ollama no respondió después de $maxRetries intentos" -ForegroundColor Red
    exit 1
}

# Paso 4: Verificar si el modelo ya está descargado
Write-Host "4. Verificando modelo llama2:latest..." -ForegroundColor Yellow
$models = Invoke-RestMethod -Uri "http://localhost:11434/api/tags"

if ($models.models.name -contains "llama2:latest") {
    Write-Host "   ✅ Modelo ya está descargado" -ForegroundColor Green
} else {
    # Paso 5: Descargar el modelo
    Write-Host "5. Descargando modelo llama2:latest..." -ForegroundColor Yellow
    Write-Host "   ⚠️ Esto puede tomar 10-30 minutos..." -ForegroundColor Magenta
    
    $body = @{
        name = "llama2:latest"
    } | ConvertTo-Json

    try {
        Invoke-RestMethod -Uri "http://localhost:11434/api/pull" -Method Post -Body $body -ContentType "application/json" | Out-Null
        Write-Host "   ✅ Modelo descargado exitosamente" -ForegroundColor Green
    }
    catch {
        Write-Host "   ❌ Error descargando el modelo: $($_.Exception.Message)" -ForegroundColor Red
        exit 1
    }
}

# Paso 6: Verificación final
Write-Host "6. Verificación final..." -ForegroundColor Yellow
try {
    $body = @{
        model = "llama2:latest"
        prompt = "Hola"
        stream = $false
    } | ConvertTo-Json

    $response = Invoke-RestMethod -Uri "http://localhost:11434/api/generate" -Method Post -Body $body -ContentType "application/json"
    Write-Host "   ✅ Ollama inicializado correctamente" -ForegroundColor Green
}
catch {
    Write-Host "   ❌ Error en verificación final: $($_.Exception.Message)" -ForegroundColor Red
    exit 1
}

Write-Host "🎉 Inicialización completada! Ahora puedes ejecutar: docker-compose up" -ForegroundColor Green
