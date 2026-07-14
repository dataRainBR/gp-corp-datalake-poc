# RBAC Setup - Grants aditivos para AnalystRole
# NAO remove IAM_ALLOWED_PRINCIPALS - preserva acessos existentes

$analystRole = "arn:aws:iam::892748149777:role/AnalystRole-gpcorp"
$glueRole = "arn:aws:iam::892748149777:role/GlueServiceRole-gpcorp"

$readOnlyDbs = @("gpcorp_gold_vendas","gpcorp_gold_cotacoes","gpcorp_gold_cadastros","gpcorp_gold_estoque","gpcorp_silver")

function Write-JsonFile($path, $content) {
    [System.IO.File]::WriteAllText("$PWD\$path", $content)
}

Write-Host "RBAC Setup - Grants aditivos"

foreach ($db in $readOnlyDbs) {
    Write-Host ""
    Write-Host "[$db]"

    $dbJson = '{"Principal":{"DataLakePrincipalIdentifier":"' + $analystRole + '"},"Resource":{"Database":{"Name":"' + $db + '"}},"Permissions":["DESCRIBE"]}'
    Write-JsonFile "cli/temp_grant_db.json" $dbJson
    $r1 = aws lakeformation grant-permissions --cli-input-json "file://cli/temp_grant_db.json" 2>&1
    Write-Host "  DESCRIBE database: $r1"

    $tblJson = '{"Principal":{"DataLakePrincipalIdentifier":"' + $analystRole + '"},"Resource":{"Table":{"DatabaseName":"' + $db + '","TableWildcard":{}}},"Permissions":["SELECT","DESCRIBE"]}'
    Write-JsonFile "cli/temp_grant_tbl.json" $tblJson
    $r2 = aws lakeformation grant-permissions --cli-input-json "file://cli/temp_grant_tbl.json" 2>&1
    Write-Host "  SELECT+DESCRIBE tables: $r2"
}

Write-Host ""
Write-Host "[gpcorp_bronze - Engineer]"

$bronzeDbJson = '{"Principal":{"DataLakePrincipalIdentifier":"' + $glueRole + '"},"Resource":{"Database":{"Name":"gpcorp_bronze"}},"Permissions":["ALL"]}'
Write-JsonFile "cli/temp_grant_bronze_db.json" $bronzeDbJson
$r3 = aws lakeformation grant-permissions --cli-input-json "file://cli/temp_grant_bronze_db.json" 2>&1
Write-Host "  ALL database: $r3"

$bronzeTblJson = '{"Principal":{"DataLakePrincipalIdentifier":"' + $glueRole + '"},"Resource":{"Table":{"DatabaseName":"gpcorp_bronze","TableWildcard":{}}},"Permissions":["ALL"]}'
Write-JsonFile "cli/temp_grant_bronze_tbl.json" $bronzeTblJson
$r4 = aws lakeformation grant-permissions --cli-input-json "file://cli/temp_grant_bronze_tbl.json" 2>&1
Write-Host "  ALL tables: $r4"

Remove-Item "cli/temp_grant_db.json" -ErrorAction SilentlyContinue
Remove-Item "cli/temp_grant_tbl.json" -ErrorAction SilentlyContinue
Remove-Item "cli/temp_grant_bronze_db.json" -ErrorAction SilentlyContinue
Remove-Item "cli/temp_grant_bronze_tbl.json" -ErrorAction SilentlyContinue

Write-Host ""
Write-Host "RBAC configurado. IAM_ALLOWED_PRINCIPALS nao foi alterado."
Write-Host "Acessos existentes QuickSight e Glue Jobs permanecem intactos."
