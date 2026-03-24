# ML offline evaluation report

- Generated at: `2026-03-24 03:57:15 UTC`
- Snapshot ID: `train_1774311326`
- Train rows: `1250476`
- Validation rows: `312620`
- Validation date range: `2026-01-31 06:39:08+00:00` to `2026-03-24 00:15:26+00:00`

## Executive summary

- Outcome: Promote `tilt_risk_baseline` only.
- `win_probability_baseline`: blocked (f1 (0.6546 < 0.6600); brier (0.2101 > 0.2000)).
- `impact_percentile_baseline`: blocked (rmse (20.2318 > 20.0000)).
- `tilt_risk_baseline`: approved (all promotion gates pass).
- Win operational rate: at threshold `0.45`, predicted-positive volume is `52.88%` of validation rows.
- Tilt operational rate: at threshold `0.40`, predicted-positive volume is `3.36%` of validation rows.

### Decision block

| model                      | decision | reason                                        |
| -------------------------- | -------- | --------------------------------------------- |
| win_probability_baseline   | blocked  | f1 (0.6546 < 0.6600); brier (0.2101 > 0.2000) |
| impact_percentile_baseline | blocked  | rmse (20.2318 > 20.0000)                      |
| tilt_risk_baseline         | approved | all promotion gates pass                      |

## Promotion gates

| model_name                 | gate_name      | comparator | target  | actual  | status |
| -------------------------- | -------------- | ---------- | ------- | ------- | ------ |
| win_probability_baseline   | f1             | >=         | 0.6600  | 0.6546  | FAIL   |
| win_probability_baseline   | brier          | <=         | 0.2000  | 0.2101  | FAIL   |
| win_probability_baseline   | min_fold_f1    | >=         | 0.6000  | 0.6216  | PASS   |
| impact_percentile_baseline | rmse           | <=         | 20.0000 | 20.2318 | FAIL   |
| impact_percentile_baseline | mae            | <=         | 16.0000 | 15.3376 | PASS   |
| impact_percentile_baseline | max_fold_rmse  | <=         | 22.0000 | 20.8100 | PASS   |
| tilt_risk_baseline         | f1             | >=         | 0.8500  | 0.9342  | PASS   |
| tilt_risk_baseline         | brier          | <=         | 0.0200  | 0.0037  | PASS   |
| tilt_risk_baseline         | recall         | >=         | 0.9500  | 0.9780  | PASS   |
| tilt_risk_baseline         | fold_f1_stddev | <=         | 0.0300  | 0.0051  | PASS   |

## Key operating numbers

### Temporal fold minima and variance

| model                      | min_fold_f1 | fold_f1_stddev | max_fold_rmse | fold_rmse_stddev |
| -------------------------- | ----------- | -------------- | ------------- | ---------------- |
| win_probability_baseline   | 0.6216      | 0.0019         |               |                  |
| tilt_risk_baseline         | 0.8714      | 0.0051         |               |                  |
| impact_percentile_baseline |             |                | 20.8100       | 0.2973           |

### Selected thresholds and expected volume

| model                    | threshold | min_precision_target | min_recall_target | selected_by_constraints | precision | recall | f1     | predicted_positive_rate |
| ------------------------ | --------- | -------------------- | ----------------- | ----------------------- | --------- | ------ | ------ | ----------------------- |
| win_probability_baseline | 0.4500    | 0.6000               | 0.5500            | 1.0000                  | 0.6174    | 0.6965 | 0.6546 | 0.5288                  |
| tilt_risk_baseline       | 0.4000    | 0.7500               | 0.9500            | 1.0000                  | 0.8941    | 0.9780 | 0.9342 | 0.0336                  |

### Calibration quality

| model                    | brier  | ece    |
| ------------------------ | ------ | ------ |
| win_probability_baseline | 0.2101 | 0.0017 |
| tilt_risk_baseline       | 0.0037 | 0.0031 |

## Dataset profile

| metric                   | value     |
| ------------------------ | --------- |
| players_total            | 7263.0000 |
| distinct_maps            | 537.0000  |
| distinct_match_days      | 53.0000   |
| label_win_positive_rate  | 0.4687    |
| label_tilt_positive_rate | 0.0307    |

## Model summary

| model                    | precision | recall | f1     | roc_auc | pr_auc | brier  |
| ------------------------ | --------- | ------ | ------ | ------- | ------ | ------ |
| win_probability_baseline | 0.6174    | 0.6965 | 0.6546 | 0.7235  | 0.6953 | 0.2101 |
| tilt_risk_baseline       | 0.8941    | 0.9780 | 0.9342 | 0.9989  | 0.9440 | 0.0037 |

| model                      | rmse    | mae     |
| -------------------------- | ------- | ------- |
| impact_percentile_baseline | 20.2318 | 15.3376 |

## Temporal backtesting

### Win model folds

| fold       | train_rows   | val_rows    | precision | recall | f1     | roc_auc | pr_auc | brier  |
| ---------- | ------------ | ----------- | --------- | ------ | ------ | ------- | ------ | ------ |
| fold_60_10 | 937857.0000  | 156309.0000 | 0.6489    | 0.5965 | 0.6216 | 0.7251  | 0.6969 | 0.2094 |
| fold_70_10 | 1094167.0000 | 156309.0000 | 0.6498    | 0.6025 | 0.6253 | 0.7284  | 0.6991 | 0.2083 |
| fold_80_20 | 1250476.0000 | 312619.0000 | 0.6453    | 0.6045 | 0.6243 | 0.7238  | 0.6959 | 0.2100 |

### Tilt model folds

| fold       | train_rows   | val_rows    | precision | recall | f1     | roc_auc | pr_auc | brier  |
| ---------- | ------------ | ----------- | --------- | ------ | ------ | ------- | ------ | ------ |
| fold_60_10 | 937857.0000  | 156309.0000 | 0.7773    | 1.0000 | 0.8747 | 0.9988  | 0.9467 | 0.0074 |
| fold_70_10 | 1094167.0000 | 156309.0000 | 0.7720    | 1.0000 | 0.8714 | 0.9989  | 0.9475 | 0.0073 |
| fold_80_20 | 1250476.0000 | 312619.0000 | 0.7878    | 1.0000 | 0.8813 | 0.9989  | 0.9434 | 0.0064 |

### Impact model folds

| fold       | train_rows   | val_rows    | rmse    | mae     |
| ---------- | ------------ | ----------- | ------- | ------- |
| fold_60_10 | 937857.0000  | 156309.0000 | 20.6410 | 15.8297 |
| fold_70_10 | 1094167.0000 | 156309.0000 | 20.8100 | 16.0573 |
| fold_80_20 | 1250476.0000 | 312619.0000 | 20.2318 | 15.3376 |

## Segment quality

### Win model by momentum

| momentum_label | rows        | precision | recall | f1     | roc_auc | pr_auc | brier  | positive_rate |
| -------------- | ----------- | --------- | ------ | ------ | ------- | ------ | ------ | ------------- |
| stable         | 288498.0000 | 0.6168    | 0.6974 | 0.6546 | 0.7229  | 0.6947 | 0.2103 | 0.4689        |
| hot            | 13426.0000  | 0.6247    | 0.6903 | 0.6558 | 0.7316  | 0.7075 | 0.2075 | 0.4682        |
| cold           | 10696.0000  | 0.6255    | 0.6821 | 0.6526 | 0.7289  | 0.6975 | 0.2083 | 0.4647        |

### Tilt model by momentum

| momentum_label | rows        | precision | recall | f1     | roc_auc | pr_auc | brier  | positive_rate |
| -------------- | ----------- | --------- | ------ | ------ | ------- | ------ | ------ | ------------- |
| stable         | 288498.0000 | 0.8942    | 0.9790 | 0.9347 | 0.9989  | 0.9438 | 0.0037 | 0.0305        |
| hot            | 13426.0000  | 0.9083    | 0.9660 | 0.9363 | 0.9987  | 0.9370 | 0.0039 | 0.0328        |
| cold           | 10696.0000  | 0.8757    | 0.9678 | 0.9194 | 0.9989  | 0.9561 | 0.0043 | 0.0320        |

### Impact model by momentum

| momentum_label | rows        | rmse    | mae     |
| -------------- | ----------- | ------- | ------- |
| stable         | 288498.0000 | 19.9472 | 15.1991 |
| hot            | 13426.0000  | 23.7645 | 16.9822 |
| cold           | 10696.0000  | 22.8614 | 17.0092 |

## Threshold trade-offs

### Win model

| threshold | precision | recall | f1     | predicted_positive_rate | meets_precision_target | meets_recall_target | meets_policy_constraints | policy_selected |
| --------- | --------- | ------ | ------ | ----------------------- | ---------------------- | ------------------- | ------------------------ | --------------- |
| 0.2000    | 0.5043    | 0.9764 | 0.6651 | 0.9075                  | 0.0000                 | 1.0000              | 0.0000                   | 0.0000          |
| 0.3000    | 0.5340    | 0.9284 | 0.6780 | 0.8149                  | 0.0000                 | 1.0000              | 0.0000                   | 0.0000          |
| 0.4000    | 0.5869    | 0.7874 | 0.6726 | 0.6288                  | 0.0000                 | 1.0000              | 0.0000                   | 0.0000          |
| 0.4500    | 0.6174    | 0.6965 | 0.6546 | 0.5288                  | 1.0000                 | 1.0000              | 1.0000                   | 1.0000          |
| 0.5000    | 0.6464    | 0.6025 | 0.6237 | 0.4369                  | 1.0000                 | 1.0000              | 1.0000                   | 0.0000          |
| 0.6000    | 0.7125    | 0.4018 | 0.5138 | 0.2643                  | 1.0000                 | 0.0000              | 0.0000                   | 0.0000          |
| 0.7000    | 0.8086    | 0.1923 | 0.3107 | 0.1115                  | 1.0000                 | 0.0000              | 0.0000                   | 0.0000          |

### Tilt model

| threshold | precision | recall | f1     | predicted_positive_rate | meets_precision_target | meets_recall_target | meets_policy_constraints | policy_selected |
| --------- | --------- | ------ | ------ | ----------------------- | ---------------------- | ------------------- | ------------------------ | --------------- |
| 0.1000    | 0.7788    | 1.0000 | 0.8756 | 0.0394                  | 1.0000                 | 1.0000              | 1.0000                   | 0.0000          |
| 0.2000    | 0.8232    | 0.9996 | 0.9028 | 0.0373                  | 1.0000                 | 1.0000              | 1.0000                   | 0.0000          |
| 0.3000    | 0.8658    | 0.9944 | 0.9257 | 0.0352                  | 1.0000                 | 1.0000              | 1.0000                   | 0.0000          |
| 0.4000    | 0.8941    | 0.9780 | 0.9342 | 0.0336                  | 1.0000                 | 1.0000              | 1.0000                   | 1.0000          |
| 0.5000    | 0.9135    | 0.9479 | 0.9304 | 0.0318                  | 1.0000                 | 0.0000              | 0.0000                   | 0.0000          |
| 0.6000    | 0.9250    | 0.8979 | 0.9113 | 0.0298                  | 1.0000                 | 0.0000              | 0.0000                   | 0.0000          |

## Calibration tables

- Bins are fixed-width probability buckets over `[0, 1]`; sparse bins indicate concentrated predictions.

### Win model

| bin    | rows       | min_prob | max_prob | avg_predicted | avg_observed |
| ------ | ---------- | -------- | -------- | ------------- | ------------ |
| 0.0000 | 8647.0000  | 0.0000   | 0.1000   | 0.0035        | 0.0027       |
| 1.0000 | 20256.0000 | 0.1001   | 0.2000   | 0.1703        | 0.1699       |
| 2.0000 | 28956.0000 | 0.2000   | 0.3000   | 0.2421        | 0.2429       |
| 3.0000 | 58184.0000 | 0.3000   | 0.4000   | 0.3552        | 0.3550       |
| 4.0000 | 59985.0000 | 0.4000   | 0.5000   | 0.4487        | 0.4516       |
| 5.0000 | 53955.0000 | 0.5000   | 0.6000   | 0.5482        | 0.5451       |
| 6.0000 | 47789.0000 | 0.6000   | 0.7000   | 0.6437        | 0.6424       |
| 7.0000 | 21300.0000 | 0.7000   | 0.8000   | 0.7599        | 0.7625       |
| 8.0000 | 9464.0000  | 0.8000   | 0.8998   | 0.8298        | 0.8323       |
| 9.0000 | 4084.0000  | 0.9000   | 1.0000   | 0.9960        | 0.9939       |

### Tilt model

| bin    | rows        | min_prob | max_prob | avg_predicted | avg_observed |
| ------ | ----------- | -------- | -------- | ------------- | ------------ |
| 0.0000 | 300302.0000 | 0.0000   | 0.0997   | 0.0002        | 0.0000       |
| 1.0000 | 669.0000    | 0.1001   | 0.1999   | 0.1486        | 0.0060       |
| 2.0000 | 632.0000    | 0.2000   | 0.3000   | 0.2489        | 0.0791       |
| 3.0000 | 524.0000    | 0.3003   | 0.3999   | 0.3510        | 0.2996       |
| 4.0000 | 539.0000    | 0.4004   | 0.5000   | 0.4522        | 0.5362       |
| 5.0000 | 642.0000    | 0.5003   | 0.5997   | 0.5497        | 0.7461       |
| 6.0000 | 696.0000    | 0.6000   | 0.6998   | 0.6502        | 0.8420       |
| 7.0000 | 891.0000    | 0.7001   | 0.8000   | 0.7528        | 0.8608       |
| 8.0000 | 1425.0000   | 0.8000   | 0.8999   | 0.8546        | 0.9200       |
| 9.0000 | 6300.0000   | 0.9000   | 1.0000   | 0.9732        | 0.9444       |

## Feature effects

- Permutation importance values are score deltas after shuffling a feature; they are ranking diagnostics, not directional causal effects.

### Win top permutation importances

| feature               | permutation_importance |
| --------------------- | ---------------------- |
| rolling_10_win_rate   | 0.1886                 |
| team                  | 0.0059                 |
| map                   | 0.0042                 |
| form_delta_kills      | 0.0023                 |
| form_delta_damage     | 0.0019                 |
| rolling_10_kda_ratio  | 0.0012                 |
| rolling_5_avg_kills   | 0.0011                 |
| rolling_10_avg_impact | 0.0009                 |
| games_played_to_date  | 0.0007                 |
| career_avg_impact     | 0.0006                 |
| career_avg_damage     | 0.0003                 |
| career_avg_kills      | 0.0003                 |

### Tilt top permutation importances

| feature                        | permutation_importance |
| ------------------------------ | ---------------------- |
| negative_lexicon_hits          | 0.3140                 |
| deaths                         | 0.0054                 |
| negative_chat_ratio            | 0.0001                 |
| rolling_10_negative_chat_ratio | 0.0001                 |
| map                            | 0.0001                 |
| chat_messages                  | 0.0000                 |
| all_caps_messages              | 0.0000                 |
| avg_message_length             | 0.0000                 |
| impact_index                   | 0.0000                 |
| form_delta_impact              | 0.0000                 |
| games_played_to_date           | 0.0000                 |
| rolling_10_win_rate            | 0.0000                 |

### Impact top permutation importances

| feature               | permutation_importance |
| --------------------- | ---------------------- |
| rolling_10_avg_impact | 7.4935                 |
| map                   | 3.4857                 |
| rolling_5_avg_kills   | 1.1762                 |
| career_avg_impact     | 0.4142                 |
| rolling_10_kda_ratio  | 0.3303                 |
| career_avg_damage     | 0.2668                 |
| career_avg_kills      | 0.1798                 |
| rolling_10_avg_damage | 0.0869                 |
| games_played_to_date  | 0.0594                 |
| form_delta_kills      | 0.0524                 |
| rolling_10_win_rate   | 0.0432                 |
| form_delta_damage     | 0.0415                 |

## Calibration notes

- Win and impact models now use train-time feature quality controls: rare map bucketing and quantile clipping on numeric outliers.
- Win uses a calibrated gradient-boosted classifier and impact uses a non-linear gradient-boosted regressor.
- Temporal backtesting rows above should be treated as the baseline promotion signal, not a single split metric.
- Win Brier/ECE: `0.2101` / `0.0017` at threshold `0.45`.
- Tilt Brier/ECE: `0.0037` / `0.0031` at threshold `0.40`.
- Promotion stage transitions are blocked when gate rows above include FAIL.
