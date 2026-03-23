# ML offline evaluation report

- Generated at: `2026-03-23 09:27:59 UTC`
- Snapshot ID: `train_1774255878`
- Train rows: `408111`
- Validation rows: `102028`
- Validation date range: `2026-03-05 17:31:48+00:00` to `2026-03-23 08:51:18+00:00`

## Dataset profile

| metric                   | value     |
| ------------------------ | --------- |
| players_total            | 5131.0000 |
| distinct_maps            | 280.0000  |
| distinct_match_days      | 19.0000   |
| label_win_positive_rate  | 0.4714    |
| label_tilt_positive_rate | 0.0306    |

## Model summary

| model                    | precision | recall | f1     | roc_auc | pr_auc | brier  |
| ------------------------ | --------- | ------ | ------ | ------- | ------ | ------ |
| win_probability_baseline | 1.0000    | 1.0000 | 1.0000 | 1.0000  | 1.0000 | 0.0000 |
| tilt_risk_baseline       | 0.7986    | 1.0000 | 0.8880 | 0.9981  | 0.9053 | 0.0073 |

| model                      | rmse    | mae    |
| -------------------------- | ------- | ------ |
| impact_percentile_baseline | 11.0001 | 7.7225 |

## Threshold trade-offs

### Win model

| threshold | precision | recall | f1     | predicted_positive_rate |
| --------- | --------- | ------ | ------ | ----------------------- |
| 0.3000    | 1.0000    | 1.0000 | 1.0000 | 0.4714                  |
| 0.4000    | 1.0000    | 1.0000 | 1.0000 | 0.4714                  |
| 0.5000    | 1.0000    | 1.0000 | 1.0000 | 0.4714                  |
| 0.6000    | 1.0000    | 1.0000 | 1.0000 | 0.4714                  |
| 0.7000    | 1.0000    | 1.0000 | 1.0000 | 0.4714                  |

### Tilt model

| threshold | precision | recall | f1     | predicted_positive_rate |
| --------- | --------- | ------ | ------ | ----------------------- |
| 0.2000    | 0.7292    | 1.0000 | 0.8434 | 0.0419                  |
| 0.3000    | 0.7474    | 1.0000 | 0.8554 | 0.0409                  |
| 0.4000    | 0.7718    | 1.0000 | 0.8712 | 0.0396                  |
| 0.5000    | 0.7986    | 1.0000 | 0.8880 | 0.0383                  |
| 0.6000    | 0.8117    | 1.0000 | 0.8961 | 0.0377                  |

## Calibration tables

### Win model

| bin    | rows       | min_prob | max_prob | avg_predicted | avg_observed |
| ------ | ---------- | -------- | -------- | ------------- | ------------ |
| 0.0000 | 10203.0000 | 0.0000   | 0.0000   | 0.0000        | 0.0000       |
| 1.0000 | 10203.0000 | 0.0000   | 0.0000   | 0.0000        | 0.0000       |
| 2.0000 | 10203.0000 | 0.0000   | 0.0000   | 0.0000        | 0.0000       |
| 3.0000 | 10202.0000 | 0.0000   | 0.0000   | 0.0000        | 0.0000       |
| 4.0000 | 10203.0000 | 0.0000   | 0.0046   | 0.0008        | 0.0000       |
| 5.0000 | 10203.0000 | 0.0046   | 0.9980   | 0.7141        | 0.7142       |
| 6.0000 | 10202.0000 | 0.9980   | 1.0000   | 0.9990        | 1.0000       |
| 7.0000 | 10203.0000 | 1.0000   | 1.0000   | 1.0000        | 1.0000       |
| 8.0000 | 20406.0000 | 1.0000   | 1.0000   | 1.0000        | 1.0000       |

### Tilt model

| bin    | rows       | min_prob | max_prob | avg_predicted | avg_observed |
| ------ | ---------- | -------- | -------- | ------------- | ------------ |
| 0.0000 | 10203.0000 | 0.0000   | 0.0005   | 0.0004        | 0.0000       |
| 1.0000 | 10203.0000 | 0.0005   | 0.0006   | 0.0005        | 0.0000       |
| 2.0000 | 10203.0000 | 0.0006   | 0.0007   | 0.0007        | 0.0000       |
| 3.0000 | 10202.0000 | 0.0007   | 0.0009   | 0.0008        | 0.0000       |
| 4.0000 | 10203.0000 | 0.0009   | 0.0010   | 0.0009        | 0.0000       |
| 5.0000 | 10203.0000 | 0.0010   | 0.0012   | 0.0011        | 0.0000       |
| 6.0000 | 10202.0000 | 0.0012   | 0.0014   | 0.0013        | 0.0000       |
| 7.0000 | 10203.0000 | 0.0014   | 0.0017   | 0.0015        | 0.0000       |
| 8.0000 | 10203.0000 | 0.0017   | 0.0022   | 0.0019        | 0.0000       |
| 9.0000 | 10203.0000 | 0.0022   | 1.0000   | 0.3897        | 0.3059       |

## Top coefficients

### Win positive weights

| feature                         | weight  |
| ------------------------------- | ------- |
| num\_\_score_delta              | 13.3331 |
| num\_\_team_score               | 10.9023 |
| num\_\_rolling_10_win_rate      | 0.5630  |
| cat\_\_map_pl_upward_f12        | 0.5023  |
| cat\_\_map_pl_vigil_rc10        | 0.4088  |
| cat\_\_map_koth_product_final   | 0.2765  |
| cat\_\_map_pass_arena2_b16g     | 0.2161  |
| cat\_\_map_pl_swiftwater_final1 | 0.1911  |
| num\_\_kda_ratio                | 0.1842  |
| cat\_\_map_ultiduo_baloo_v2     | 0.1764  |
| num\_\_damage_share_of_team     | 0.1633  |
| num\_\_healing_share_of_team    | 0.1411  |

### Win negative weights

| feature                         | weight   |
| ------------------------------- | -------- |
| num\_\_opponent_score           | -10.8861 |
| cat\_\_momentum_label_stable    | -1.3831  |
| cat\_\_team_Red                 | -1.1545  |
| cat\_\_team_Blue                | -1.1131  |
| cat\_\_map_cp_sunshine          | -0.7318  |
| cat\_\_map_cp_process_f12       | -0.7000  |
| cat\_\_map_cp_gullywash_f9      | -0.6974  |
| num\_\_damage_dealt             | -0.6327  |
| num\_\_damage_per_minute        | -0.6316  |
| cat\_\_map_cp_snakewater_final1 | -0.5897  |
| cat\_\_momentum_label_hot       | -0.4593  |
| cat\_\_momentum_label_cold      | -0.4251  |

### Tilt positive weights

| feature                          | weight |
| -------------------------------- | ------ |
| num\_\_negative_lexicon_hits     | 3.9339 |
| num\_\_deaths                    | 0.4493 |
| cat\_\_map_pass_arena2_b16g      | 0.4238 |
| cat\_\_map_ultiduo_baloo_v2      | 0.3626 |
| cat\_\_map_koth_bagel_rc11       | 0.2855 |
| cat\_\_map_cp_sultry_b8a         | 0.2416 |
| cat\_\_map_cp_metalworks_f5      | 0.2054 |
| cat\_\_map_cp_sunshine           | 0.1926 |
| cat\_\_map_cp_snakewater_final1  | 0.1880 |
| cat\_\_map_cp_granary_pro_rc17a3 | 0.1713 |
| cat\_\_map_cp_process_f12        | 0.1502 |
| cat\_\_map_cp_subbase_b3a        | 0.1323 |

### Tilt negative weights

| feature                       | weight  |
| ----------------------------- | ------- |
| cat\_\_team_Red               | -1.4682 |
| cat\_\_team_Blue              | -1.4530 |
| cat\_\_momentum_label_cold    | -1.1350 |
| cat\_\_map_pl_upward_f12      | -0.9647 |
| cat\_\_momentum_label_stable  | -0.9191 |
| cat\_\_momentum_label_hot     | -0.8672 |
| cat\_\_map_ctf_ballin_skyfall | -0.7450 |
| cat\_\_map_pl_vigil_rc10      | -0.6300 |
| num\_\_negative_chat_ratio    | -0.6093 |
| cat\_\_map_pass_pball_a33     | -0.6083 |
| cat\_\_map_ctf_bball_comptf   | -0.3735 |
| num\_\_score_delta            | -0.2896 |

### Impact positive weights

| feature                      | weight  |
| ---------------------------- | ------- |
| cat\_\_map_pass_yard_b1      | 38.9719 |
| cat\_\_map_pass_pibble_a3    | 32.9712 |
| cat\_\_map_pass_yard_a5      | 30.2945 |
| cat\_\_map_tfdb_spacebox_a2  | 26.1738 |
| cat\_\_map_hns_mall_a3       | 25.8481 |
| cat\_\_map_tfdb_box_space_a6 | 23.5203 |
| cat\_\_map_pass_yard_a3      | 22.8078 |
| cat\_\_map_pass_pball_a33    | 21.1403 |
| cat\_\_map_pass_yard_a5a     | 18.2011 |
| cat\_\_map_pass_boutique_rc5 | 17.7421 |
| cat\_\_map_pass_yard_a1      | 17.3631 |
| cat\_\_map_koth_cachoeira    | 17.0284 |

### Impact negative weights

| feature                             | weight   |
| ----------------------------------- | -------- |
| cat\_\_map_koth_ultiduo_r_b7        | -51.4143 |
| cat\_\_map_ultiduo_baloo_v2         | -49.2237 |
| cat\_\_map_ultiduo_baloo_v1488_d    | -42.2163 |
| cat\_\_map_ultiduo_lookout_b1       | -39.5117 |
| cat\_\_map_ultiduo_baloo            | -34.3218 |
| cat\_\_map_ultiduo_baloo_v2_noholog | -33.9506 |
| cat\_\_map_ultiduo_coaltown_v8      | -30.1002 |
| cat\_\_map_ultiduo_grove_b4         | -28.5457 |
| cat\_\_map_ultiduo_seclusion_b3     | -26.6470 |
| cat\_\_map_ultiduo_gullywash_b2     | -24.0214 |
| cat\_\_map_ultiduo_acropolis_b2     | -21.9851 |
| cat\_\_map_koth_slasher             | -21.1281 |

## Calibration notes

- Win Brier score: `0.0000`. If production thresholding matters, tune threshold against business costs.
- Tilt Brier score: `0.0073`. Class imbalance is handled with `class_weight=balanced`; calibrate with isotonic regression before production promotion.
- Impact RMSE/MAE are baseline quality only; assess residuals by map and class usage slices before staging promotion.
