# Test script to verify the COVID-19 Data Quality Monitoring System setup
# Run this after installation to check if everything is configured correctly

Write-Host "=" -NoNewline -ForegroundColor Cyan
Write-Host ("=" * 69) -ForegroundColor Cyan
Write-Host " COVID-19 Data Quality Monitoring System - Setup Verification" -ForegroundColor Cyan
Write-Host "=" -NoNewline -ForegroundColor Cyan
Write-Host ("=" * 69) -ForegroundColor Cyan
Write-Host ""

$allPassed = $true

# Check Python version
Write-Host "[1/7] Checking Python version..." -ForegroundColor Yellow
try {
    $pythonVersion = python --version 2>&1
    Write-Host "  Found: $pythonVersion" -ForegroundColor Green
    if ($pythonVersion -match "Python 3\.([8-9]|[1-9][0-9])") {
        Write-Host "  Python version OK" -ForegroundColor Green
    } else {
        Write-Host "  WARNING: Python 3.8+ recommended" -ForegroundColor Red
        $allPassed = $false
    }
} catch {
    Write-Host "  ERROR: Python not found" -ForegroundColor Red
    $allPassed = $false
}
Write-Host ""

# Check directory structure
Write-Host "[2/7] Checking directory structure..." -ForegroundColor Yellow
$requiredDirs = @(
    "data\raw",
    "data\validated", 
    "data\quarantine",
    "scripts",
    "config",
    "logs"
)

$dirCheckPassed = $true
foreach ($dir in $requiredDirs) {
    if (Test-Path $dir) {
        Write-Host "  $dir - OK" -ForegroundColor Green
    } else {
        Write-Host "  $dir - MISSING" -ForegroundColor Red
        $dirCheckPassed = $false
    }
}

if ($dirCheckPassed) {
    Write-Host "  Directory structure OK" -ForegroundColor Green
} else {
    Write-Host "  ERROR: Some directories missing" -ForegroundColor Red
    $allPassed = $false
}
Write-Host ""

# Check required files
Write-Host "[3/7] Checking required files..." -ForegroundColor Yellow
$requiredFiles = @(
    "requirements.txt",
    "config\config.yaml",
    "scripts\data_ingestion.py",
    "scripts\validation_pipeline.py",
    "main.py"
)

$fileCheckPassed = $true
foreach ($file in $requiredFiles) {
    if (Test-Path $file) {
        Write-Host "  $file - OK" -ForegroundColor Green
    } else {
        Write-Host "  $file - MISSING" -ForegroundColor Red
        $fileCheckPassed = $false
    }
}

if ($fileCheckPassed) {
    Write-Host "  Required files OK" -ForegroundColor Green
} else {
    Write-Host "  ERROR: Some files missing" -ForegroundColor Red
    $allPassed = $false
}
Write-Host ""

# Check Python packages
Write-Host "[4/7] Checking Python packages..." -ForegroundColor Yellow
$requiredPackages = @("pandas", "great_expectations", "requests", "pyyaml", "duckdb")
$packageCheckPassed = $true

foreach ($package in $requiredPackages) {
    $result = python -c "import $package" 2>&1
    if ($LASTEXITCODE -eq 0) {
        Write-Host "  $package - OK" -ForegroundColor Green
    } else {
        Write-Host "  $package - NOT INSTALLED" -ForegroundColor Red
        $packageCheckPassed = $false
    }
}

if ($packageCheckPassed) {
    Write-Host "  All required packages installed" -ForegroundColor Green
} else {
    Write-Host "  ERROR: Some packages missing. Run: pip install -r requirements.txt" -ForegroundColor Red
    $allPassed = $false
}
Write-Host ""

# Check configuration
Write-Host "[5/7] Checking configuration..." -ForegroundColor Yellow
if (Test-Path "config\config.yaml") {
    $config = Get-Content "config\config.yaml" -Raw
    if ($config -match "data_source:" -and $config -match "paths:" -and $config -match "validation:") {
        Write-Host "  Configuration file valid" -ForegroundColor Green
    } else {
        Write-Host "  WARNING: Configuration may be incomplete" -ForegroundColor Yellow
    }
} else {
    Write-Host "  ERROR: Configuration file missing" -ForegroundColor Red
    $allPassed = $false
}
Write-Host ""

# Check environment variables (optional)
Write-Host "[6/7] Checking environment variables (optional)..." -ForegroundColor Yellow
if (Test-Path ".env") {
    Write-Host "  .env file found" -ForegroundColor Green
    $env = Get-Content ".env" -Raw
    if ($env -match "EMAIL_PASSWORD" -or $env -match "SLACK_WEBHOOK_URL") {
        Write-Host "  Alert configuration detected" -ForegroundColor Green
    } else {
        Write-Host "  INFO: No alert credentials configured" -ForegroundColor Cyan
    }
} else {
    Write-Host "  INFO: .env file not found (optional)" -ForegroundColor Cyan
    Write-Host "  INFO: Copy .env.example to .env to enable alerts" -ForegroundColor Cyan
}
Write-Host ""

# Test data download (optional)
Write-Host "[7/7] Testing data source connectivity..." -ForegroundColor Yellow
try {
    $url = "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/owid-covid-data.csv"
    $response = Invoke-WebRequest -Uri $url -Method Head -TimeoutSec 10
    if ($response.StatusCode -eq 200) {
        Write-Host "  Data source accessible" -ForegroundColor Green
    } else {
        Write-Host "  WARNING: Data source returned status $($response.StatusCode)" -ForegroundColor Yellow
    }
} catch {
    Write-Host "  WARNING: Could not reach data source (check internet connection)" -ForegroundColor Yellow
}
Write-Host ""

# Final summary
Write-Host "=" -NoNewline -ForegroundColor Cyan
Write-Host ("=" * 69) -ForegroundColor Cyan
Write-Host " SUMMARY" -ForegroundColor Cyan
Write-Host "=" -NoNewline -ForegroundColor Cyan
Write-Host ("=" * 69) -ForegroundColor Cyan

if ($allPassed) {
    Write-Host "`nAll critical checks passed!" -ForegroundColor Green
    Write-Host "`nNext steps:" -ForegroundColor Cyan
    Write-Host "  1. Run setup: python main.py --setup --define-expectations" -ForegroundColor White
    Write-Host "  2. Test pipeline: python main.py" -ForegroundColor White
    Write-Host "  3. Review QUICKSTART.md for detailed usage" -ForegroundColor White
} else {
    Write-Host "`nSome checks failed. Please review errors above." -ForegroundColor Red
    Write-Host "`nRecommended actions:" -ForegroundColor Cyan
    Write-Host "  1. Install missing packages: pip install -r requirements.txt" -ForegroundColor White
    Write-Host "  2. Verify directory structure" -ForegroundColor White
    Write-Host "  3. Check Python version (3.8+ required)" -ForegroundColor White
}

Write-Host ""
Write-Host "=" -NoNewline -ForegroundColor Cyan
Write-Host ("=" * 69) -ForegroundColor Cyan
