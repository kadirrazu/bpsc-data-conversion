<?php

include 'file-to-convert/cadre-list-array.php';

$sqlRows = [];

foreach ($general_cadres as $cadre_type => $cadres) {

    foreach ($cadres as $abbr => $info) {

        $code = $info['code'];
        $name = addslashes($info['name']); // escape quotes

        $sqlRows[] = sprintf(
            "(%d, '%s', '%s', '%s', NULL)",
            $code,
            $abbr,
            $name,
            $cadre_type
        );
    }
}

$finalSQL =
"INSERT INTO `cadres`
(`cadre_code`, `cadre_abbr`, `cadre_name`, `cadre_type`, `subject_requirements`)
VALUES\n" . implode(",\n", $sqlRows) . ";";

// Save to file
$file = 'conversion-output/cadres_sql.txt';
file_put_contents($file, $finalSQL);

echo "SQL export saved to $file";