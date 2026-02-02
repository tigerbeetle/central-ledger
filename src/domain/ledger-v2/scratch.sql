SELECT 
`settlement_window`.`id`,
       `settlement_window`.`state`,
       `settlement_window`.`reason`, 
       `settlement_window`.`opened_at`, 
       `settlement_window`.`closed_at`, 
       settlement_balance.* 
       `tigerbeetleSettlement`.`id` as `settlement_id`
FROM `tigerbeetleSettlementWindow` AS `settlement_window` 
  LEFT JOIN `tigerbeetleSettlementWindowMapping` AS `settlement_window_mapping` 
    ON `settlement_window_mapping`.`window_id` = `settlement_window`.`id` 
  LEFT JOIN `tigerbeetleSettlementBalance` AS `settlement_balance` 
    ON `settlement_balance`.`settlement_id` = `settlement_window_mapping`.`settlement_id` 
  JOIN `tigerbeetleSettlement`
    ON `tigerbeetleSettlement`.`id` = `settlement_window_mapping`.`settlement_id`
WHERE `settlement_window`.`opened_at` >= '1970-01-01T00:00:00.000Z'
ORDER BY `settlement_window`.`opened_at` DESC;




SELECT tigerbeetle_settlement.*
FROM `tigerbeetleSettlementWindow` AS `settlement_window` 
  LEFT JOIN `tigerbeetleSettlementWindowMapping` AS `settlement_window_mapping` 
    ON `settlement_window_mapping`.`window_id` = `settlement_window`.`id` 
  LEFT JOIN `tigerbeetleSettlementBalance` AS `settlement_balance` 
    ON `settlement_balance`.`settlement_id` = `settlement_window_mapping`.`settlement_id` 
  JOIN `tigerbeetleSettlement` as `tigerbeetle_settlement`
    ON `tigerbeetle_settlement`.`id` = `settlement_window_mapping`.`settlement_id`
WHERE `settlement_window`.`opened_at` >= '1970-01-01T00:00:00.000Z'
ORDER BY `settlement_window`.`opened_at` DESC \G

SELECT 
  settlement_window.id as settlement_window_id,
  settlement_window.state as settlement_window_state,
  settlement_window.opened_at as settlement_window_opened_at,
  settlement_window.closed_at as settlement_window_closed_at,
  settlement_window.reason as settlement_window_reason,
  settlement_window.created_at as settlement_window_created_at,
  settlement_balance.id as settlement_balance_id,
  settlement_balance.settlement_id as settlement_balance_settlement_id,
  settlement_balance.dfspId as settlement_balance_dfspId,
  settlement_balance.currency as settlement_balance_currency,
  settlement_balance.amount as settlement_balance_amount,
  settlement_balance.direction as settlement_balance_direction,
  settlement_balance.state as settlement_balance_state,
  settlement_balance.external_reference as settlement_balance_external_reference,
  settlement_balance.created_at as settlement_balance_created_at,
  settlement_balance.updated_at as settlement_balance_updated_at,
  settlement.id as settlement_id,
  settlement.state as settlement_state,
  settlement.model as settlement_model,
  settlement.reason as settlement_reason,
  settlement.created_at as settlement_created_at
FROM `tigerbeetleSettlementWindow` AS `settlement_window` 
  LEFT JOIN `tigerbeetleSettlementWindowMapping` AS `settlement_window_mapping` 
    ON `settlement_window_mapping`.`window_id` = `settlement_window`.`id` 
  LEFT JOIN `tigerbeetleSettlementBalance` AS `settlement_balance` 
    ON `settlement_balance`.`settlement_id` = `settlement_window_mapping`.`settlement_id` 
  JOIN `tigerbeetleSettlement` AS `settlement`
    ON `settlement`.`id` = `settlement_window_mapping`.`settlement_id`
WHERE `settlement_window`.`opened_at` >= '1970-01-01T00:00:00.000Z'
ORDER BY `settlement_window`.`opened_at` DESC;


