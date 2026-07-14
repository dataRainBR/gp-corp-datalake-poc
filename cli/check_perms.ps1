# Verifica permissoes Lake Formation por database
$dbs = @("gpcorp_bronze","gpcorp_silver","gpcorp_gold_vendas","gpcorp_gold_cotacoes","gpcorp_gold_cadastros","gpcorp_gold_estoque","gpcorp_metrics","gpcorp_silver_elementary","gpcorp_silver_gold_dbt_test_audit","forecast")

foreach ($db in $dbs) {
    Write-Host "`n=== $db ===" -ForegroundColor Cyan
    $resFile = "cli/temp_res_check.json"
    @{ Database = @{ Name = $db } } | ConvertTo-Json -Depth 5 -Compress | Set-Content $resFile -NoNewline

    $result = aws lakeformation list-permissions --resource "file://$resFile" --output json 2>$null
    if ($result) {
        $parsed = $result | ConvertFrom-Json
        if ($parsed.PrincipalResourcePermissions.Count -eq 0) {
            Write-Host "  (sem grants LF explicitos - pode estar em modo IAM_ALLOWED_PRINCIPALS default)" -ForegroundColor Yellow
        } else {
            foreach ($p in $parsed.PrincipalResourcePermissions) {
                $principal = $p.Principal.DataLakePrincipalIdentifier
                $perms = $p.Permissions -join ","
                Write-Host "  $principal : $perms"
            }
        }
    } else {
        Write-Host "  ERRO ao consultar" -ForegroundColor Red
    }
}
Remove-Item "cli/temp_res_check.json" -ErrorAction SilentlyContinue
