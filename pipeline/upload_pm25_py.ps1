$territories = @("concepcion_py","san_pedro_py","cordillera_py","guaira_py","caaguazu_py","caazapa_py","misiones_py","paraguari_py","central_py","neembucu_py","amambay_py","canindeyu_py")

foreach ($t in $territories) {
    $global = "pipeline\output\$t\sat_pm25_drivers.parquet"
    if (Test-Path $global) {
        Write-Host "Uploading $t global..."
        npx wrangler r2 object put "neahub/data/$t/sat_pm25_drivers.parquet" --file $global --remote
        # Per-dpto
        $dpto_dir = "pipeline\output\$t\sat_dpto"
        if (Test-Path $dpto_dir) {
            Get-ChildItem "$dpto_dir\sat_pm25_drivers_*.parquet" | ForEach-Object {
                npx wrangler r2 object put "neahub/data/$t/sat_dpto/$($_.Name)" --file $_.FullName --remote
            }
        }
    } else {
        Write-Host "SKIP $t - no parquet"
    }
}
Write-Host "Done"
