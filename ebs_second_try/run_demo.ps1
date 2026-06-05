# Script PowerShell pentru o demonstrație rapidă a sistemului
# Salvați acest fișier ca: run_demo.ps1

Write-Host ""
Write-Host "======================================================================" -ForegroundColor Cyan
Write-Host "     SISTEM PUBLISH/SUBSCRIBE CU FILTRARE PE CONTINUT" -ForegroundColor Yellow
Write-Host "                    DEMONSTRATIE RAPIDA" -ForegroundColor Cyan
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
Write-Host "Se ruleaza demonstratia..." -ForegroundColor Green
Write-Host ""

# Script Python pentru demonstrație
$pythonScript = @"
import sys
import os
import time
import random

# Adaugă src la path
sys.path.insert(0, os.path.join(os.getcwd(), 'src'))

from broker import BrokerNetwork
from subscriber import Subscriber
from publisher import Publisher
from data_generator import DataGenerator

print('\n=== DEMONSTRATIE SISTEMA ===\n')

# Initializare retea
gen = DataGenerator()
broker_network = BrokerNetwork(num_brokers=3)

# Creare subscriberi
subs = [Subscriber(f'sub_{i}', broker_network.get_broker_addresses()) for i in range(2)]

# Inregistrare subscriptii
print('Inregistrare subscriptii:')
result1 = subs[0].subscribe({'company': ('=', 'Google')})
result2 = subs[0].subscribe({'value': ('>', 200)})
result3 = subs[1].subscribe({'company': ('=', 'Microsoft')})
print('  ✓ 3 subscriptii inregistrate\n')

# Creare publisher
pub = Publisher('demo_pub', broker_network.get_broker_addresses())

# Publicare mesaje
print('Publicare mesaje:')
for i in range(5):
    pub_data = gen.generate_publication()
    success, latency = pub.publish(pub_data)
    status = '✓' if success else '✗'
    print(f'  Mesaj {i+1}: {pub_data["company"]} = {pub_data["value"]} | Status: {status} | Latenta: {latency:.2f}ms')
    time.sleep(0.5)

print('\n=== Demonstratie finalizata ===\n')
broker_network.stop_all()
"@

# Salvează scriptul temporar și rulează-l
$tempFile = [System.IO.Path]::GetTempFileName() + ".py"
$pythonScript | Out-File -FilePath $tempFile -Encoding UTF8
python $tempFile
Remove-Item $tempFile

Write-Host ""
Write-Host "======================================================================" -ForegroundColor Cyan
Write-Host "                     DEMONSTRATIE FINALIZATA" -ForegroundColor Green
Write-Host "======================================================================" -ForegroundColor Cyan
Write-Host ""

Read-Host "Apasati Enter pentru a inchide"