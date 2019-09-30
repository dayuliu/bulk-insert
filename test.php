<?php
require_once 'vendor/autoload.php';

use bulkinsert\Query;
use Doctrine\DBAL\Configuration;

$config = new Configuration();
$connectionParams = array(
    'dbname' => 'bw',
    'user' => 'homework',
    'password' => 'homework',
    'host' => '192.168.10.38',
    'driver' => 'pdo_mysql',
);
$connectionParams = array(
    'dbname' => 'bw',
    'user' => 'root',
    'password' => 'root',
    'host' => '127.0.0.1',
    'driver' => 'pdo_mysql',
);
$connection = \Doctrine\DBAL\DriverManager::getConnection($connectionParams, $config);

$rows = (new Query($connection))->execute('product', [
    ['id' => 111, 'bigid' => 222,'varchar'=>123,'xiaoshu'=>'10.2'],
    ['id' => 2222, 'bigid' => 222,'varchar'=>123,'xiaoshu'=>'10.2'],
]);