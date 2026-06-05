
Write-Host ""
Write-Host "======================================================================" -ForegroundColor Cyan
Write-Host "     SISTEM PUBLISH/SUBSCRIBE CU FILTRARE PE CONTINUT" -ForegroundColor Yellow
Write-Host "                    RULARE EVALUARE COMPLETA" -ForegroundColor Cyan
Write-Host "======================================================================" -ForegroundColor Cyan
Write-Host ""

try {
    $pythonVersion = python --version 2>&1
    Write-Host "✓ Python detectat: $pythonVersion" -ForegroundColor Green
} catch {
    Write-Host "✗ Python nu este instalat sau nu se afla in PATH" -ForegroundColor Red
    Write-Host "  Instalati Python de la https://www.python.org/downloads/" -ForegroundColor Yellow
    exit 1
}

if (-not (Test-Path "src\evaluator.py")) {
    Write-Host "✗ Nu se gasesc fisierele sursa in directorul src/" -ForegroundColor Red
    Write-Host "  Asigurati-va ca toate fisierele .py sunt in directorul src/" -ForegroundColor Yellow
    exit 1
}

Write-Host ""
Write-Host "Pornire evaluare sistem..." -ForegroundColor Green
Write-Host ""

$pythonScript = @"
import sys
import os

# Adaugă src la path
sys.path.insert(0, os.path.join(os.getcwd(), 'src'))

from evaluator import SystemEvaluator

if __name__ == "__main__":
    evaluator = SystemEvaluator()
    evaluator.run_evaluation()
"@

$tempFile = [System.IO.Path]::GetTempFileName() + ".py"
$pythonScript | Out-File -FilePath $tempFile -Encoding UTF8
python $tempFile
Remove-Item $tempFile

Write-Host ""
Write-Host "======================================================================" -ForegroundColor Cyan
Write-Host "                     EVALUARE FINALIZATA" -ForegroundColor Green
Write-Host "======================================================================" -ForegroundColor Cyan
Write-Host ""

Read-Host "Apasati Enter pentru a inchide"