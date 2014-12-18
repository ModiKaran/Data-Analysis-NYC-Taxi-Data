<?php

$arr1 = array();
$arr2 = array();

$file1 = fopen("month/" . $_GET["month_id"] . ".txt", "r");
while(!feof($file1))
{
	$line = fgets($file1);
	$a = explode("\t",$line);
	if($arr1[(int)$a[0]]==Null)
	{
		$arr1[(int)$a[0]] = (int)$a[1];
	}
}
fclose($file1);


$file2 = fopen("op2.txt", "r");
$i = 0;
while(!feof($file2))
{
        $arr2[$i] = fgets($file2);
	$i += 1;
}
fclose($file2);

$finalarr = array('arr1'=>$arr1,'arr2'=>$arr2);

print_r(json_encode($finalarr));

?>
