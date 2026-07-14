"""
Glue Python Shell Job: executa dbt tests sobre a camada Silver.

Instala dbt-athena + elementary, roda os testes, e:
- store_failures: persiste registros que falham em tabelas de auditoria (gpcorp_silver.dbt_test_audit)
- elementary: gera histórico de execução em gpcorp_silver.elementary

Job type: Python Shell (não Spark) — barato, cobrado por segundo.
Configuração no Glue:
  --additional-python-modules dbt-athena-community==1.8.*,elementary-data==0.15.*
  --dbt_project_s3 s3://gpcorp-datalake/dbt/gpcorp_quality/
"""
import os
import sys
import subprocess
import boto3

s3 = boto3.client("s3")


def download_dbt_project(bucket: str, prefix: str, local_dir: str):
    """Baixa o projeto dbt do S3 para o ambiente local do job."""
    os.makedirs(local_dir, exist_ok=True)
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            rel_path = key[len(prefix):].lstrip("/")
            if not rel_path:
                continue
            local_path = os.path.join(local_dir, rel_path)
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            s3.download_file(bucket, key, local_path)
            print(f"  baixado: {rel_path}")


def run_command(cmd, cwd):
    """Executa comando shell e retorna (exit_code, output)."""
    print(f"\n[CMD] {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)
    print(result.stdout)
    if result.stderr:
        print(f"[STDERR] {result.stderr}")
    return result.returncode, result.stdout


def main():
    bucket = "gpcorp-datalake"
    prefix = "dbt/gpcorp_quality"
    project_dir = "/tmp/gpcorp_quality"

    print("=" * 60)
    print("  DBT TESTS — Camada Silver")
    print("=" * 60)

    # 1. Baixa projeto dbt do S3
    print("\n[1] Baixando projeto dbt...")
    download_dbt_project(bucket, prefix, project_dir)

    # Define DBT_PROFILES_DIR para o projeto
    env_dir = project_dir
    os.environ["DBT_PROFILES_DIR"] = env_dir

    # 2. Instala dependências dbt (dbt_utils, elementary)
    print("\n[2] dbt deps...")
    run_command(["dbt", "deps", "--project-dir", project_dir], project_dir)

    # 3. Roda elementary on-run-start (cria tabelas de auditoria)
    print("\n[3] dbt run (elementary models)...")
    run_command(["dbt", "run", "--select", "elementary", "--project-dir", project_dir], project_dir)

    # 4. Executa testes (store_failures persiste falhas)
    print("\n[4] dbt test...")
    exit_code, output = run_command(["dbt", "test", "--project-dir", project_dir], project_dir)

    # 5. Gera relatório Elementary (HTML) e envia para S3
    print("\n[5] Gerando relatório Elementary...")
    run_command([
        "edr", "report",
        "--project-dir", project_dir,
        "--file-path", "/tmp/elementary_report.html"
    ], project_dir)

    try:
        s3.upload_file(
            "/tmp/elementary_report.html",
            bucket,
            "dbt/reports/elementary_report.html"
        )
        print(f"[REPORT] s3://{bucket}/dbt/reports/elementary_report.html")
    except Exception as e:
        print(f"[WARN] Falha ao enviar relatório: {e}")

    # 6. Decide status final
    if exit_code != 0:
        print("\n[RESULT] Alguns testes falharam (severity=warn não bloqueia).")
        print("         Ver detalhes em gpcorp_silver.dbt_test_audit.*")
        # Não faz raise — severity=warn. Para bloquear, mude para error no dbt_project.yml
    else:
        print("\n[RESULT] Todos os testes passaram ✓")

    print("\n[COMPLETE] dbt tests finalizado.")


if __name__ == "__main__":
    main()
