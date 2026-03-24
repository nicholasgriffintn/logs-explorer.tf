# ML offline evaluation report

- Generated at: `2026-03-24 02:28:30 UTC`
- Snapshot ID: `train_1774311326`
- Train rows: `1250476`
- Validation rows: `312620`
- Validation date range: `2026-01-31 06:39:08+00:00` to `2026-03-24 00:15:26+00:00`

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
| win_probability_baseline | 0.6426    | 0.6038 | 0.6226 | 0.7200  | 0.6916 | 0.2120 |
| tilt_risk_baseline       | 0.7846    | 1.0000 | 0.8793 | 0.9981  | 0.9024 | 0.0076 |

| model                      | rmse    | mae     |
| -------------------------- | ------- | ------- |
| impact_percentile_baseline | 22.0605 | 17.7460 |

## Temporal backtesting

### Win model folds

| fold       | train_rows   | val_rows    | precision | recall | f1     | roc_auc | pr_auc | brier  |
| ---------- | ------------ | ----------- | --------- | ------ | ------ | ------- | ------ | ------ |
| fold_60_10 | 937857.0000  | 156309.0000 | 0.6457    | 0.5984 | 0.6211 | 0.7218  | 0.6929 | 0.2113 |
| fold_70_10 | 1094167.0000 | 156309.0000 | 0.6470    | 0.5996 | 0.6224 | 0.7242  | 0.6946 | 0.2105 |
| fold_80_20 | 1250476.0000 | 312619.0000 | 0.6426    | 0.6038 | 0.6226 | 0.7200  | 0.6916 | 0.2120 |

### Tilt model folds

| fold       | train_rows   | val_rows    | precision | recall | f1     | roc_auc | pr_auc | brier  |
| ---------- | ------------ | ----------- | --------- | ------ | ------ | ------- | ------ | ------ |
| fold_60_10 | 937857.0000  | 156309.0000 | 0.7743    | 1.0000 | 0.8728 | 0.9976  | 0.8873 | 0.0088 |
| fold_70_10 | 1094167.0000 | 156309.0000 | 0.7676    | 1.0000 | 0.8685 | 0.9978  | 0.8964 | 0.0089 |
| fold_80_20 | 1250476.0000 | 312619.0000 | 0.7846    | 1.0000 | 0.8793 | 0.9981  | 0.9024 | 0.0076 |

### Impact model folds

| fold       | train_rows   | val_rows    | rmse    | mae     |
| ---------- | ------------ | ----------- | ------- | ------- |
| fold_60_10 | 937857.0000  | 156309.0000 | 22.1831 | 17.9589 |
| fold_70_10 | 1094167.0000 | 156309.0000 | 22.6349 | 18.4456 |
| fold_80_20 | 1250476.0000 | 312619.0000 | 22.0604 | 17.7459 |

## Segment quality

### Win model by momentum

| momentum_label | rows        | precision | recall | f1     | roc_auc | pr_auc | brier  | positive_rate |
| -------------- | ----------- | --------- | ------ | ------ | ------- | ------ | ------ | ------------- |
| stable         | 288498.0000 | 0.6420    | 0.6044 | 0.6226 | 0.7194  | 0.6909 | 0.2122 | 0.4689        |
| hot            | 13426.0000  | 0.6523    | 0.6020 | 0.6261 | 0.7291  | 0.7040 | 0.2089 | 0.4682        |
| cold           | 10696.0000  | 0.6463    | 0.5903 | 0.6170 | 0.7245  | 0.6930 | 0.2105 | 0.4647        |

### Tilt model by momentum

| momentum_label | rows        | precision | recall | f1     | roc_auc | pr_auc | brier  | positive_rate |
| -------------- | ----------- | --------- | ------ | ------ | ------- | ------ | ------ | ------------- |
| stable         | 288498.0000 | 0.7869    | 1.0000 | 0.8807 | 0.9982  | 0.9076 | 0.0074 | 0.0305        |
| hot            | 13426.0000  | 0.7750    | 1.0000 | 0.8733 | 0.9969  | 0.8543 | 0.0090 | 0.0328        |
| cold           | 10696.0000  | 0.7403    | 1.0000 | 0.8507 | 0.9968  | 0.8446 | 0.0105 | 0.0320        |

### Impact model by momentum

| momentum_label | rows        | rmse    | mae     |
| -------------- | ----------- | ------- | ------- |
| stable         | 288498.0000 | 21.5228 | 17.4222 |
| hot            | 13426.0000  | 30.1611 | 23.9578 |
| cold           | 10696.0000  | 24.2428 | 18.6834 |

## Threshold trade-offs

### Win model

| threshold | precision | recall | f1     | predicted_positive_rate |
| --------- | --------- | ------ | ------ | ----------------------- |
| 0.3000    | 0.5461    | 0.8928 | 0.6777 | 0.7663                  |
| 0.4000    | 0.5924    | 0.7622 | 0.6667 | 0.6030                  |
| 0.5000    | 0.6426    | 0.6038 | 0.6226 | 0.4404                  |
| 0.6000    | 0.6996    | 0.4275 | 0.5307 | 0.2864                  |
| 0.7000    | 0.7687    | 0.2480 | 0.3750 | 0.1512                  |

### Tilt model

| threshold | precision | recall | f1     | predicted_positive_rate |
| --------- | --------- | ------ | ------ | ----------------------- |
| 0.2000    | 0.7215    | 1.0000 | 0.8382 | 0.0425                  |
| 0.3000    | 0.7393    | 1.0000 | 0.8501 | 0.0415                  |
| 0.4000    | 0.7599    | 1.0000 | 0.8636 | 0.0404                  |
| 0.5000    | 0.7846    | 1.0000 | 0.8793 | 0.0391                  |
| 0.6000    | 0.8038    | 1.0000 | 0.8912 | 0.0382                  |

## Calibration tables

### Win model

| bin    | rows       | min_prob | max_prob | avg_predicted | avg_observed |
| ------ | ---------- | -------- | -------- | ------------- | ------------ |
| 0.0000 | 31262.0000 | 0.0261   | 0.2045   | 0.1515        | 0.1299       |
| 1.0000 | 31262.0000 | 0.2045   | 0.2804   | 0.2429        | 0.2630       |
| 2.0000 | 31262.0000 | 0.2804   | 0.3498   | 0.3102        | 0.3371       |
| 3.0000 | 31262.0000 | 0.3498   | 0.4013   | 0.3765        | 0.3971       |
| 4.0000 | 31262.0000 | 0.4013   | 0.4682   | 0.4310        | 0.4437       |
| 5.0000 | 31262.0000 | 0.4682   | 0.5193   | 0.4943        | 0.4931       |
| 6.0000 | 31262.0000 | 0.5193   | 0.5915   | 0.5562        | 0.5421       |
| 7.0000 | 31262.0000 | 0.5915   | 0.6578   | 0.6208        | 0.6005       |
| 8.0000 | 31262.0000 | 0.6578   | 0.7437   | 0.7003        | 0.6652       |
| 9.0000 | 31262.0000 | 0.7437   | 0.9994   | 0.8122        | 0.8157       |

### Tilt model

| bin    | rows       | min_prob | max_prob | avg_predicted | avg_observed |
| ------ | ---------- | -------- | -------- | ------------- | ------------ |
| 0.0000 | 31262.0000 | 0.0000   | 0.0005   | 0.0004        | 0.0000       |
| 1.0000 | 31262.0000 | 0.0005   | 0.0006   | 0.0005        | 0.0000       |
| 2.0000 | 31262.0000 | 0.0006   | 0.0007   | 0.0006        | 0.0000       |
| 3.0000 | 31262.0000 | 0.0007   | 0.0008   | 0.0008        | 0.0000       |
| 4.0000 | 31262.0000 | 0.0008   | 0.0010   | 0.0009        | 0.0000       |
| 5.0000 | 31262.0000 | 0.0010   | 0.0012   | 0.0011        | 0.0000       |
| 6.0000 | 31262.0000 | 0.0012   | 0.0014   | 0.0013        | 0.0000       |
| 7.0000 | 31262.0000 | 0.0014   | 0.0017   | 0.0015        | 0.0000       |
| 8.0000 | 31262.0000 | 0.0017   | 0.0023   | 0.0019        | 0.0000       |
| 9.0000 | 31262.0000 | 0.0023   | 1.0000   | 0.3957        | 0.3069       |

## Top coefficients

### Win positive weights

| feature                        | weight |
| ------------------------------ | ------ |
| cat\_\_map_9 v 27              | 1.0629 |
| cat\_\_map_koth_berry_b3a      | 1.0413 |
| num\_\_rolling_10_win_rate     | 0.9468 |
| cat\_\_map_gullywash + vigil   | 0.8171 |
| cat\_\_map_ultitrio_bound_rc1a | 0.7061 |
| cat\_\_map_koth_jobby_a1       | 0.6937 |
| cat\_\_map_koth_ashville       | 0.6841 |
| cat\_\_map_koth_harvest        | 0.6732 |
| cat\_\_map_gullywash + govan   | 0.6714 |
| cat\_\_map_cp_process_F12      | 0.6594 |
| cat\_\_map_cp_metalworks_rc7   | 0.6468 |
| cat\_\_map_cp_process_f7       | 0.6450 |

### Win negative weights

| feature                             | weight  |
| ----------------------------------- | ------- |
| cat\_\_map_product + ashville       | -1.6810 |
| cat\_\_map_Steel                    | -1.2835 |
| cat\_\_map_Nidal                    | -1.2732 |
| cat\_\_map_Switfwater               | -1.2329 |
| cat\_\_map_cp_steel_f12 2x          | -1.2231 |
| cat\_\_map_ashville + product       | -1.1307 |
| cat\_\_map_Vigil x2                 | -1.0826 |
| cat\_*map*я завидую димору          | -1.0649 |
| cat\__map_(Заебался)                | -1.0553 |
| cat\_\_map_LOL                      | -1.0331 |
| cat\_\_map_At least he got some ZZZ | -1.0241 |
| cat\_\_map_Upward/Ashville/Vigil    | -1.0225 |

### Tilt positive weights

| feature                         | weight |
| ------------------------------- | ------ |
| num\_\_negative_lexicon_hits    | 3.7996 |
| num\_\_deaths                   | 0.5542 |
| cat\_\_map_cp_sunshine          | 0.1451 |
| cat\_\_map_cp_snakewater_final1 | 0.0972 |
| cat\_\_map_cp_gullywash_f9      | 0.0529 |
| cat\_\_map_pass_arena2_b15      | 0.0451 |
| cat\_\_map_koth_clearcut_b17    | 0.0420 |
| num\_\_rolling_10_win_rate      | 0.0320 |
| cat\_\_map_pass_arena2_b16g     | 0.0265 |
| num\_\_avg_message_length       | 0.0211 |
| cat\_\_map_cp_reckoner_rc6      | 0.0169 |
| num\_\_all_caps_messages        | 0.0140 |

### Tilt negative weights

| feature                      | weight  |
| ---------------------------- | ------- |
| cat\_\_team_Red              | -1.7637 |
| cat\_\_team_Blue             | -1.7471 |
| cat\_\_momentum_label_cold   | -1.2959 |
| cat\_\_momentum_label_hot    | -1.2683 |
| cat\_\_momentum_label_stable | -0.9466 |
| num\_\_negative_chat_ratio   | -0.5687 |
| cat\_\_map_pl_vigil_rc10     | -0.5089 |
| num\_\_chat_messages         | -0.4268 |
| cat\_\_map_pl_upward_f12     | -0.4256 |
| cat\_\_map_pass_pibble_a3    | -0.2929 |
| num\_\_score_delta           | -0.2767 |
| cat\_\_map_pass_pball_a33    | -0.2519 |

### Impact positive weights

| feature                             | weight  |
| ----------------------------------- | ------- |
| cat\_\_map_ultitrio_minecraft       | 37.0294 |
| cat\_\_map_ultitrio_aesthetic_b9    | 35.2052 |
| cat\_\_map_ultitrio_dockport_final6 | 34.1524 |
| cat\_\_map_ultitrio_eruption_v6     | 32.5027 |
| cat\_\_map_ultitrio_staten_rc1      | 31.1779 |
| cat\_\_map_ultitrio_swine_b02b      | 30.7727 |
| cat\_\_map_ultitrio_caffa_final9    | 30.4522 |
| cat\_\_map_koth_maple_ridge_rc2     | 30.3379 |
| cat\_\_map_ultiduo_furnace_b2       | 29.8489 |
| cat\_\_map_ultitrio_prisonyard_a4   | 29.3696 |
| cat\_\_map_ultiduo_furnace_b3       | 28.9002 |
| cat\_\_map_koth_badlands            | 28.6302 |

### Impact negative weights

| feature                        | weight   |
| ------------------------------ | -------- |
| cat\_\_map_ctf_ballin_sky      | -53.1509 |
| cat\_\_map_ctf_bball_comptf    | -48.6928 |
| cat\_\_map_pass_pibble_a3      | -47.3657 |
| cat\_\_map_bball_ozone_ozf     | -46.7117 |
| cat\_\_map_pass_pball_a33      | -46.3137 |
| cat\_\_map_pass_kribble_a10    | -44.5523 |
| cat\_\_map_pass_thesis_turbo9  | -36.6451 |
| cat\_\_map_kotf_factory_b3     | -35.3289 |
| cat\_\_map_tfdb_spacebox_a2    | -34.4730 |
| cat\_\_map_tfdb_box_space_a6   | -33.1768 |
| cat\_\_map_ctf_bball_alpine_b4 | -32.8103 |
| cat\_\_map_tfdb_octagon_odb_a1 | -32.6375 |

## Calibration notes

- Win and impact models now use pre-match form/context features only; outcome-proxy leakage features are blocked at training time.
- Temporal backtesting rows above should be treated as the baseline promotion signal, not a single split metric.
- Win Brier score: `0.2120`. If production thresholding matters, tune threshold against business costs.
- Tilt Brier score: `0.0076`. Class imbalance is handled with `class_weight=balanced`; calibrate with isotonic regression before production promotion.
- Impact RMSE/MAE are baseline quality only; assess residuals by map and class usage slices before staging promotion.
