<?php

include 'conversion-output/dbf-to-file/output_candidates_array_bcs44.php';

$sqlRows = [];

foreach ($candidates as $i => $c) {

    $sqlRows[] = sprintf(
        "('%s', '%s', '%s', '%s', '%s', '%s', %d, %d, '%s', '%s', '%s', %d, '%s')",

        // basic fields
        $c['user_id'],
        $c['reg'],
        $c['name'],
        $c['cadre_category'],
        $c['district'],
        $c['gender'],
        $c['general_merit_position'] ?? NULL,
        $c['technical_merit_position'] ?? NULL,
        $c['technical_passed_cadres'] ?? NULL,
        $c['choice_list'],
        $c['choice_list_tech'] ?? NULL,
		$c['has_quota'] ?? NULL,
		$c['quota_info'] ?? NULL,
    );
}

$finalSQL = "INSERT INTO `candidates` (`user_id`, `reg`, `name`, `cadre_category`, `district`, `gender`, `general_merit_position`, `technical_merit_position`, `technical_passed_cadres`, `choice_list`, `choice_list_tech`, `has_quota`, `quota_info`) VALUES\n"
    . implode(",\n", $sqlRows) . ";";

$file = 'conversion-output/dbf-to-file/candidates_sql_bcs44.txt';

file_put_contents($file, $finalSQL);

echo "SQL export saved to $file";