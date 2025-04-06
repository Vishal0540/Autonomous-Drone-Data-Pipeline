Write-Host "Starting all services..."

Write-Host "Starting Flink jobs..."
Set-Location flink_jobs
Start-Process powershell -ArgumentList "-NoExit", "-Command python drone_activity_job.py" -WindowStyle Normal
Start-Process powershell -ArgumentList "-NoExit", "-Command python red_zone_alert_job.py" -WindowStyle Normal
Set-Location ..

Write-Host "Waiting for Flink jobs to initialize (20 seconds)..."
Start-Sleep -Seconds 20

Write-Host "Starting Kafka consumers..."
Set-Location kafka_consumers
Start-Process powershell -ArgumentList "-NoExit", "-Command python drone_status_consumer.py" -WindowStyle Normal
Start-Process powershell -ArgumentList "-NoExit", "-Command python drone_recent_activity_consumer.py" -WindowStyle Normal
Start-Process powershell -ArgumentList "-NoExit", "-Command python red_zone_alerts_consumer.py" -WindowStyle Normal
Set-Location ..

Start-Sleep -Seconds 20

Write-Host "Starting drone simulators..."
Set-Location drone_data_simulator
Start-Process powershell -ArgumentList "-NoExit", "-Command python simulate_drones_telemetry.py --batch 1" -WindowStyle Normal
Start-Sleep -Seconds 20
# Start-Process powershell -ArgumentList "-NoExit", "-Command python simulate_drones_telemetry.py --batch 2" -WindowStyle Normal
# Start-Sleep -Seconds 20
Start-Process powershell -ArgumentList "-NoExit", "-Command python simulate_drones_telemetry.py --use-red-zone --red-zone-id 0" -WindowStyle Normal
Set-Location ..

Write-Host "All services have been started."
Write-Host "Press any key to exit this window..."
$null = $Host.UI.RawUI.ReadKey("NoEcho,IncludeKeyDown")
