# Runbook: Colab Training

Use Google Colab for compute-heavy LightGBM search phases.

## Flow

1. Export sample parquet locally.
2. Copy dataset to Google Drive.
3. Commit and push notebook/config/sample changes.
4. Open the GitHub-backed notebook in Colab.
5. Run all cells.
6. Copy model/search artifacts back to `models/<model_id>/`.
7. Verify artifacts before validation.

See also:

- `docs/modeling/lightgbm_development.md`
- `docs/engineering/tooling.md`

