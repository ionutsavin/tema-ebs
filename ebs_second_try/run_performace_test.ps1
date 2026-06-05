# Script PowerShell pentru testarea performanței generatorului cu paralelizare
# Salvați acest fișier ca: run_performance_test.ps1

Write-Host ""
Write-Host "======================================================================" -ForegroundColor Cyan
Write-Host "     SISTEM PUBLISH/SUBSCRIBE CU FILTRARE PE CONTINUT" -ForegroundColor Yellow
Write-Host "              TEST PERFORMANTA GENERATOR" -ForegroundColor Cyan
Write-Host "======================================================================" -ForegroundColor Cyan
Write-Host ""

# Verifică Python
try {
    python --version 2>&1 | Out-Null
    Write-Host "✓ Python detectat" -ForegroundColor Green
} catch {
    Write-Host "✗ Python nu este instalat" -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host "Rulare test performanta..." -ForegroundColor Green
Write-Host ""

# Script Python pentru test performanță
$pythonScript = @"
import sys
import os
import time

# Adaugă src la path
sys.path.insert(0, os.path.join(os.getcwd(), 'src'))

from data_generator import DataGenerator

print('\n=== TEST PERFORMANTA GENERATOR ===\n')
gen = DataGenerator()

print('Generare 50000 publicatii cu diferite configuratii de paralelizare:\n')
print('-' * 60)

for threads in [1, 2, 4, 8]:
    start = time.time()
    pubs = gen.generate_publications_parallel(50000, threads)
    elapsed = time.time() - start
    rate = 50000 / elapsed
    print(f'Thread-uri: {threads:2d} | Timp: {elapsed:6.3f}s | Rata: {rate:8.0f} pub/sec')
    print(f'  Distributie: {gen.generation_stats["by_thread"]}')
    print()

print('-' * 60)
print('\n=== TEST FINALIZAT ===\n')
"@

# Salvează scriptul temporar și rulează-l
$tempFile = [System.IO.Path]::GetTempFileName() + ".py"
$pythonScript | Out-File -FilePath $tempFile -Encoding UTF8
python $tempFile
Remove-Item $tempFile

Write-Host ""
Write-Host "======================================================================" -ForegroundColor Cyan
Write-Host "                     TEST FINALIZAT" -ForegroundColor Green
Write-Host "======================================================================" -ForegroundColor Cyan
Write-Host ""

Read-Host "Apasati Enter pentru a inchide"