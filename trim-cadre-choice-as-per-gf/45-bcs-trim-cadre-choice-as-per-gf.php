<!DOCTYPE html>
<html>
<head>
<style>
    table {
        /* Sets the spacing between cells to 10px */
        border-spacing: 2px; 
        border: 1px solid black;
    }

    th, td {
        /* Sets the padding inside cells to 15px */
        padding: 5px; 
        border: 1px solid gray;
    }
</style>
</head>
<body>

<?php

$keywordMap = [
    'general education' => 'EDGN',
    'polytechnic institute' => 'EDPI',
	'tech education' => 'EDPI',
	'administration' => 'ADMN',
	'foreign affairs' => 'FORN',
	'police' => 'PLIC',
	'audit & accounts' => 'AUDT',
	'taxation' => 'TAXN',
	'customs' => 'CSTM',
	'co operative' => 'COPG',
	'railway transportion' => 'RLGG',
	'postal' => 'POST',
	'family planning' => 'FAML',
	'food' => 'FODG',
	'railway engineering assistant executive engineer' => 'RLNP',
	'railway engineering assistant mechanical engineer' => 'RLME',
	'railway engineering ast mechanical engineer' => 'RLME',
	'railway engineer ast mechanical engineer' => 'RLME',
	'railway engineering ast mechanical engineer' => 'RLEE',
	'railway engineer Ast Signal & Tele' => 'RLSE',
	'forests' => 'FORT',
	'forest' => 'FORT',
	'roads & highways assistant engineer civil' => 'ROCE',
	'roads & highways assistant engineer mechanical' => 'ROME',
	'fisheries' => 'FITO',
	'veterinary surgeon' => 'VLSO',
	'livestock PDO' => 'LPDO',
	'liivestock so PDO' => 'LPDO',
	'agriculture extension officer' => 'AGEX',
	'agriculture scientific officer' => 'AGSO',
	'assistant surgeon' => 'MEDI',
	'assistant dental surgeon' => 'DENT',
	'assistant dentral surgeon' => 'DENT',
	'food assistant maintenance engineer' => 'FODT',
	'public works assistant engineer civil' => 'PWCE',
	'public works ast engineer civil' => 'PWCE',
	'public works assistant engineer e m' => 'PWEM',
	'public works ast engineer e m' => 'PWEM',
	'statistical officer' => 'STOF',
	'co operative statistician' => 'STCO',
	'family planning medical officer' => 'MEOF',
	'Public Health Ast Engineer' => 'PHAE',
	'Information ast radio engineer' => 'INEN',
	'Information Programme Organiser' => 'INPO',
	'Information Assistant Director' => 'INAD',
	'Information Asst Director' => 'INAD',
	'Information Ast Controller of News' => 'INAC',
	
];

$servername = "localhost";
$username = "root";
$password = "";
$dbname = "temp_database_tables";

//Create connection
$conn = new mysqli($servername, $username, $password, $dbname);

//Check connection
if($conn->connect_error){
  die("Connection failed: " . $conn->connect_error);
}

echo "<strong>EVENT:</strong> Connected successfully to DB." . '<br><br>';


$sql = "SELECT * FROM `candidates_45_with_gf`";

echo "<strong>EVENT:</strong> Printing List BEFORE Processing." . '<br><br>';

$result = $conn->query($sql);

if ($result->num_rows > 0) {
	
	echo '<table>';
	echo '<tr>
			<th>Sr.</th>
			<th>User ID</th>
			<th>Reg</th>
			<th>Choice List</th>
			<th>GF Text</th>
			<th>Existing Job</th>
			<th>Existing Abbr</th>
			<th>Trimmed Choice</th>
		</tr>';
		
  $counter = 1;
  
  $dataToProcess = [];
	
  // output data of each row
  while( $row = $result->fetch_assoc() )
  {
	  
	if( strtolower($row["gf_decision_text"]) == 'no')
	{
		echo "<tr>
			<td>{$counter}</td>
			<td>{$row["user_id"]}</td>
			<td>{$row["reg"]}</td>
			<td>{$row["choice_list_backup"]}</td>
			<td>{$row["gf_decision_text"]}</th>
			<td>{$row["existing_job_cadre"]}</td>
			<td>N/A</td>
			<td>N/A</td>
		</tr>";
		
		$dataToProcess[] = [
			'user_id' => $row["user_id"], 
			'reg' => $row["reg"], 
			'choice_list' => $row["choice_list_backup"], 
			'gf_decision_text' => $row["gf_decision_text"], 
			'existing_job_cadre' => $row["existing_job_cadre"]
		];
		
		$counter++;
	}
	
  }
 
	echo '</table>';
  
} else {
  echo "0 results";
}


/*PROCESS ARRAY*/

function applyKeywordAbbreviations(array $data, array $keywordMap, $textIndex = 'existing_job_cadre')
{
    foreach ($data as $i => $row) {

        if (!isset($row[$textIndex])) {
            continue;
        }

        // normalize text
        $check = strtolower($row[$textIndex]);
        $check = str_replace(['(', ')', ',', '-', '/', ',', '.', '  ', '   '], ' ', $check);
        $check = str_replace(['  ', '   ', '    '], ' ', $check);

        foreach ($keywordMap as $keyword => $abbr) {

            if (strpos(trim($check), strtolower(trim($keyword))) !== false) {

                // prevent duplicate abbreviation
                if (!in_array($abbr, $data[$i], true)) {
                    $data[$i]['existing_abbr'] = $abbr;
                }
				
				$data[$i]['trimmed_choice_list'] = processChoiceList($row['choice_list'], $abbr);

                break; // stop after first match
            }
        }
    }

    return $data;
}

function processChoiceList($choiceList, $abbr)
{
    $items = preg_split('/\s+/', trim($choiceList));

    $left = [];
    $found = false;

    foreach ($items as $item) {
		
		if( (substr($item, 0, 2) === 'ED') && ((substr($abbr, 0, 2) === 'ED') && $abbr != 'EDPI') )
		{
			$found = true;
			break;
		}
		else if( (substr($item, 2, 2) === 'PI') && (substr($abbr, 0, 2) === 'PI') )
		{			
			$found = true;
			break;
		}
		else
		{
			if ($item === $abbr) {
				$found = true;
				break;
			}
		}
        
        $left[] = $item;
    }

    return trim(implode(' ', $left));
}

function markAbbr($choiceList, $abbr)
{
    $parts = preg_split('/\s+/', trim($choiceList));

    foreach ($parts as &$p) {
        if ($p === $abbr) {
            $p = '<span class="matched-abbr">'.$p.'</span>';
            break;
        }
    }

    return implode(' ', $parts);
}

$dataToProcessOutput = applyKeywordAbbreviations($dataToProcess, $keywordMap);

echo '<br><br>';

/*
echo '<pre>';
print_r($dataToProcessOutput);
echo '</pre>';
*/

echo "<strong>EVENT:</strong> Printing List AFTER Processing." . '<br><br>';

if (count($dataToProcessOutput) > 0) {
	
	echo '<table>';
	echo '<tr>
			<th>Sr.</th>
			<th>User ID</th>
			<th>Reg</th>
			<th>Choice List</th>
			<th>GF Text</th>
			<th>Existing Job</th>
			<th>Existing Abbr</th>
			<th>Trimmed Choice</th>
		</tr>';
		
  $counter = 1;
	
  // output data of each row
  foreach( $dataToProcessOutput as $key => $val )
  {
	  
	echo "<tr>
			<td>{$counter}</td>
			<td>{$val['user_id']}</td>
			<td>{$val['reg']}</td>
			<td>{$val['choice_list']}</td>
			<td>{$val['gf_decision_text']}</th>
			<td>{$val['existing_job_cadre']}</td>
			<td>" . (isset($val['existing_abbr']) ? $val['existing_abbr'] : '-') . "</td>
			<td>" . (isset($val['trimmed_choice_list']) ? $val['trimmed_choice_list'] : '-') . "</td>
		</tr>";
		
	$counter++;
	
  }
 
	echo '</table>';
  
} else {
  echo "0 results";
}

echo '<br><br>';

echo "<strong>EVENT:</strong> Patching the result into DB." . '<br><br>';

$update_counter = 0;

foreach( $dataToProcessOutput as $key => $val )
{
	 
	$sql = "UPDATE `candidates_45_with_gf` SET choice_list = '{$val['trimmed_choice_list']}', existing_job_cadre_abbr = '{$val['existing_abbr']}' WHERE user_id = '{$val['user_id']}'";
	
	if( $conn->query($sql) === TRUE ) {
	  $update_counter++;
	}
		
}

echo $update_counter . ' records were patched successfully.';

echo '<br><br>';

$conn->close();