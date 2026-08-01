Remove-Item raft-data-*.json -ErrorAction SilentlyContinue

echo "Starting Node 1..."
Start-Process -FilePath ".\raft-kv.exe" -ArgumentList "--node-id=node1", "--http-addr=:8001", "--raft-addr=:9001", "--peers=:9001,:9002,:9003" -NoNewWindow -RedirectStandardOutput "node1.log" -RedirectStandardError "node1.err.log" -PassThru -OutVariable node1

echo "Starting Node 2..."
Start-Process -FilePath ".\raft-kv.exe" -ArgumentList "--node-id=node2", "--http-addr=:8002", "--raft-addr=:9002", "--peers=:9001,:9002,:9003" -NoNewWindow -RedirectStandardOutput "node2.log" -RedirectStandardError "node2.err.log" -PassThru -OutVariable node2

echo "Starting Node 3..."
Start-Process -FilePath ".\raft-kv.exe" -ArgumentList "--node-id=node3", "--http-addr=:8003", "--raft-addr=:9003", "--peers=:9001,:9002,:9003" -NoNewWindow -RedirectStandardOutput "node3.log" -RedirectStandardError "node3.err.log" -PassThru -OutVariable node3

echo "Waiting for 3 seconds for leader election..."
Start-Sleep -Seconds 3

echo "Testing PUT request on Node 1..."
$putResponse = Invoke-WebRequest -Uri "http://localhost:8001/store/name" -Method Put -Body '{"value": "IIT KGP"}' -ContentType "application/json" -UseBasicParsing
echo "PUT Status: $($putResponse.StatusCode)"

echo "Testing GET request on Node 2..."
$getResponse = Invoke-WebRequest -Uri "http://localhost:8002/store/name" -Method Get -UseBasicParsing
echo "GET Data: $($getResponse.Content)"

echo "Testing Consistent GET request on Node 3..."
$getConsistentResponse = Invoke-WebRequest -Uri "http://localhost:8003/store/name?consistent=true" -Method Get -UseBasicParsing
echo "GET Consistent Data: $($getConsistentResponse.Content)"

echo "Killing Node 1..."
Stop-Process -Id $node1.Id -Force

echo "Waiting for 3 seconds for re-election..."
Start-Sleep -Seconds 3

echo "Testing GET request on Node 2 after Node 1 killed..."
$getResponseAfterKill = Invoke-WebRequest -Uri "http://localhost:8002/store/name" -Method Get -UseBasicParsing
echo "GET Data After Kill: $($getResponseAfterKill.Content)"

echo "Testing PUT request on Node 3 after Node 1 killed..."
$putResponse2 = Invoke-WebRequest -Uri "http://localhost:8003/store/newkey" -Method Put -Body '{"value": "new data"}' -ContentType "application/json" -UseBasicParsing
echo "PUT Status After Kill: $($putResponse2.StatusCode)"

echo "Killing all nodes..."
Stop-Process -Id $node2.Id -Force
Stop-Process -Id $node3.Id -Force

echo "Done."
