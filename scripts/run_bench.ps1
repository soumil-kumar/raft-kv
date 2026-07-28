param(
    [string]$Binary = ".\raft-kv.exe"
)

function Cleanup {
    Get-Process -Name "raft-kv" -ErrorAction SilentlyContinue | Stop-Process -Force
    Start-Sleep -Seconds 1
    Remove-Item -Recurse -Force ".\raft-data-*" -ErrorAction SilentlyContinue
}

Cleanup

Write-Host "Starting cluster..."
$node1 = Start-Process -FilePath $Binary -ArgumentList "--node-id=node1", "--http-addr=:8001", "--raft-addr=:9001", "--peers=:9001,:9002,:9003", "--peer-http=:8001,:8002,:8003" -PassThru -WindowStyle Hidden
$node2 = Start-Process -FilePath $Binary -ArgumentList "--node-id=node2", "--http-addr=:8002", "--raft-addr=:9002", "--peers=:9001,:9002,:9003", "--peer-http=:8001,:8002,:8003" -PassThru -WindowStyle Hidden
$node3 = Start-Process -FilePath $Binary -ArgumentList "--node-id=node3", "--http-addr=:8003", "--raft-addr=:9003", "--peers=:9001,:9002,:9003", "--peer-http=:8001,:8002,:8003" -PassThru -WindowStyle Hidden

Start-Sleep -Seconds 4

Write-Host "Running benchmark (10,000 requests, 100 concurrent workers)..."
go run scripts/benchmark.go -url http://localhost:8001/store -c 100 -n 10000

Cleanup
