# PowerShell Chaos Test Script for raft-kv
# This script starts a 3-node cluster, writes data, kills the leader,
# and verifies that data survives the failover.

param(
    [string]$Binary = ".\raft-kv.exe"
)

$ErrorActionPreference = "Stop"

Write-Host ""
Write-Host "============================================" -ForegroundColor Cyan
Write-Host "  raft-kv Chaos Test" -ForegroundColor Cyan
Write-Host "  Testing fault tolerance and data survival" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan
Write-Host ""

# --- Helper functions ---
function Write-Step { param($msg) Write-Host "[STEP] $msg" -ForegroundColor Yellow }
function Write-Pass { param($msg) Write-Host "[PASS] $msg" -ForegroundColor Green }
function Write-Fail { param($msg) Write-Host "[FAIL] $msg" -ForegroundColor Red }

function Cleanup {
    Write-Host "`n[CLEANUP] Stopping all nodes..." -ForegroundColor DarkGray
    Get-Process -Name "raft-kv" -ErrorAction SilentlyContinue | Stop-Process -Force
    Start-Sleep -Seconds 1
    Remove-Item -Recurse -Force ".\data-node1", ".\data-node2", ".\data-node3" -ErrorAction SilentlyContinue
}

# Clean up from any previous run
Cleanup

$passed = 0
$failed = 0

try {
    # ============================================================
    # PHASE 1: Start a 3-node cluster
    # ============================================================
    Write-Step "Starting node1 (bootstrap leader)..."
    $node1 = Start-Process -FilePath $Binary -ArgumentList "--node-id=node1", "--http-addr=:8001", "--raft-addr=:9001", "--peers=:9001,:9002,:9003", "--peer-http=:8001,:8002,:8003" -PassThru -WindowStyle Hidden
    Start-Sleep -Seconds 4

    Write-Step "Starting node2 (joining cluster)..."
    $node2 = Start-Process -FilePath $Binary -ArgumentList "--node-id=node2", "--http-addr=:8002", "--raft-addr=:9002", "--peers=:9001,:9002,:9003", "--peer-http=:8001,:8002,:8003" -PassThru -WindowStyle Hidden
    Start-Sleep -Seconds 3

    Write-Step "Starting node3 (joining cluster)..."
    $node3 = Start-Process -FilePath $Binary -ArgumentList "--node-id=node3", "--http-addr=:8003", "--raft-addr=:9003", "--peers=:9001,:9002,:9003", "--peer-http=:8001,:8002,:8003" -PassThru -WindowStyle Hidden
    Start-Sleep -Seconds 3

    Write-Host ""
    Write-Host "Cluster started: node1, node2, node3" -ForegroundColor Cyan
    Write-Host ""

    # Find the true leader
    $leaderPort = $null
    foreach ($port in @(8001, 8002, 8003)) {
        try {
            $status = Invoke-RestMethod -Uri "http://localhost:$port/status" -Method Get -ErrorAction Stop
            if ($status.is_leader -eq $true) {
                $leaderPort = $port
                break
            }
        } catch { }
    }
    
    if (-not $leaderPort) {
        Write-Fail "Could not find initial leader!"
        exit 1
    }
    Write-Pass "Found initial leader on port $leaderPort"

    # ============================================================
    # PHASE 2: Write test data to the leader
    # ============================================================
    Write-Step "Writing test data to the cluster..."

    $testData = @{
        "city"    = "Kharagpur"
        "college" = "IIT KGP"
        "project" = "raft-kv"
    }

    foreach ($key in $testData.Keys) {
        $value = $testData[$key]
        $body = @{ value = $value } | ConvertTo-Json
        $response = Invoke-RestMethod -Uri "http://localhost:$leaderPort/store/$key" -Method Put -Body $body -ContentType "application/json"
        Write-Host "  SET $key = $value -> $($response.status)" -ForegroundColor DarkGray
    }
    Write-Pass "All test data written successfully"
    $passed++

    # ============================================================
    # PHASE 3: Verify data is replicated to all nodes
    # ============================================================
    Write-Step "Verifying data replication across all nodes..."
    Start-Sleep -Seconds 2

    $allReplicated = $true
    foreach ($port in @(8001, 8002, 8003)) {
        foreach ($key in $testData.Keys) {
            try {
                $response = Invoke-RestMethod -Uri "http://localhost:$port/store/$key" -Method Get
                if ($response.value -ne $testData[$key]) {
                    Write-Fail "Node :$port has wrong value for $key (expected $($testData[$key]), got $($response.value))"
                    $allReplicated = $false
                }
            } catch {
                Write-Fail "Node :$port failed to return $key"
                $allReplicated = $false
            }
        }
    }

    if ($allReplicated) {
        Write-Pass "All data correctly replicated to all 3 nodes"
        $passed++
    } else {
        Write-Fail "Replication check failed"
        $failed++
    }

    # ============================================================
    # PHASE 4: Kill the leader!
    # ============================================================
    Write-Host ""
    Write-Host "  killing the leader (port $leaderPort)!" -ForegroundColor Red
    Write-Host ""
    
    $processes = @{
        "8001" = $node1
        "8002" = $node2
        "8003" = $node3
    }
    $leaderProc = $processes["$leaderPort"]
    Stop-Process -Id $leaderProc.Id -Force
    Write-Step "Leader on port $leaderPort killed. Waiting for re-election..."
    Start-Sleep -Seconds 5

    # ============================================================
    # PHASE 5: Verify a new leader was elected
    # ============================================================
    Write-Step "Checking for new leader..."
    $newLeaderFound = $false
    
    $survivingPorts = @(8001, 8002, 8003) | Where-Object { $_ -ne $leaderPort }

    foreach ($port in $survivingPorts) {
        try {
            $status = Invoke-RestMethod -Uri "http://localhost:$port/status" -Method Get
            if ($status.is_leader -eq $true) {
                Write-Pass "New leader elected: node on port $port"
                $newLeaderFound = $true
                break
            }
        } catch {
            # Node might be temporarily unavailable during election
        }
    }

    if (-not $newLeaderFound) {
        Write-Host "  No node reports as leader yet, checking data accessibility..." -ForegroundColor DarkGray
    }

    # ============================================================
    # PHASE 6: Verify data survived the leader failure
    # ============================================================
    Write-Step "Verifying data survived leader failure..."
    
    $dataSurvived = $true
    foreach ($port in $survivingPorts) {
        foreach ($key in $testData.Keys) {
            try {
                $response = Invoke-RestMethod -Uri "http://localhost:$port/store/$key" -Method Get
                if ($response.value -ne $testData[$key]) {
                    Write-Fail "Node :$port has wrong value for $key after leader death"
                    $dataSurvived = $false
                }
            } catch {
                Write-Fail "Node :$port cannot serve $key after leader death"
                $dataSurvived = $false
            }
        }
    }

    if ($dataSurvived) {
        Write-Pass "ALL DATA SURVIVED THE LEADER FAILURE!"
        $passed++
    } else {
        Write-Fail "Data was lost during leader failure"
        $failed++
    }

    # ============================================================
    # PHASE 7: Write new data to the new leader
    # ============================================================
    Write-Step "Writing new data to surviving cluster..."
    
    $writeSuccess = $false
    foreach ($port in $survivingPorts) {
        try {
            $body = @{ value = "survives" } | ConvertTo-Json
            $response = Invoke-RestMethod -Uri "http://localhost:$port/store/chaos_test" -Method Put -Body $body -ContentType "application/json"
            if ($response.status -eq "ok") {
                Write-Pass "Successfully wrote to the cluster after leader failure (port $port)"
                $writeSuccess = $true
                break
            }
        } catch {
            # This node might be a follower and forwarding failed. Try the other one.
        }
    }

    if ($writeSuccess) { $passed++ } else { Write-Fail "Could not write to cluster after leader failure"; $failed++ }

    # ============================================================
    # RESULTS
    # ============================================================
    Write-Host ""
    Write-Host "============================================" -ForegroundColor Cyan
    Write-Host "  CHAOS TEST RESULTS" -ForegroundColor Cyan
    Write-Host "============================================" -ForegroundColor Cyan
    Write-Host "  Passed: $passed" -ForegroundColor Green
    Write-Host "  Failed: $failed" -ForegroundColor $(if ($failed -gt 0) { "Red" } else { "Green" })
    Write-Host "============================================" -ForegroundColor Cyan
    Write-Host ""

    if ($failed -eq 0) {
        Write-Host "  ALL TESTS PASSED! Your distributed KV store is fault-tolerant." -ForegroundColor Green
    } else {
        Write-Host "  SOME TESTS FAILED. Check the logs above for details." -ForegroundColor Red
    }

} finally {
    Cleanup
}
