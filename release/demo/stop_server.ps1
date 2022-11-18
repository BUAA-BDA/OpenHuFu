$raw = (NETSTAT.EXE -ano | findstr "0.0.0.0:1234")
if ([String]::IsNullOrEmpty($raw))
{
    Write-Output "server port process not found"
    return
}
$raw = $raw.split("\n")
for ($i = 0; $i -lt $raw.Count; $i++) {
    $id = $raw[$i].split(" ")[37]
    taskkill.exe /PID $id /F
    $index = $i+1
    Remove-Item log/$index.log
    Remove-Item log/pid_$index
    Write-Output "successfully stopped server-$index"
}
Write-Output "end"