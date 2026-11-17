-- MySQL dump 10.13  Distrib 8.4.11, for Linux (x86_64)
--
-- Host: localhost    Database: andesstay
-- ------------------------------------------------------
-- Server version	8.4.11

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!50503 SET NAMES utf8mb4 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `audit_events`
--

DROP TABLE IF EXISTS `audit_events`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `audit_events` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `action` varchar(255) NOT NULL,
  `description` varchar(255) NOT NULL,
  `event_date` datetime(6) NOT NULL,
  `reservation_id` bigint NOT NULL,
  `user` varchar(255) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=23 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `audit_events`
--

LOCK TABLES `audit_events` WRITE;
/*!40000 ALTER TABLE `audit_events` DISABLE KEYS */;
INSERT INTO `audit_events` VALUES (1,'RESERVA_CREADA','Se creó la reserva para el huésped Prueba Final','2026-09-20 22:27:04.354791',1,'cris.vergarac@duocuc.cl'),(2,'RESERVA_CREADA','Se creó la reserva para el huésped lolaso','2026-09-20 22:28:14.488141',2,'cris.vergarac@duocuc.cl'),(3,'RESERVA_CREADA','Se creó la reserva para el huésped sasa','2026-09-20 22:38:55.273064',3,'cris.vergarac@duocuc.cl'),(4,'RESERVA_CONFIRMADA','La reserva cambió de CREADA a CONFIRMADA','2026-09-20 23:04:52.756267',1,'cris.vergarac@duocuc.cl'),(5,'RESERVA_CREADA','Se creó la reserva para el huésped Prueba POST DIRECTO','2026-09-21 16:10:37.667353',13,'cris.vergarac@duocuc.cl'),(6,'RESERVA_CREADA','Se creó la reserva para el huésped Prueba POST API Gateway','2026-09-21 16:11:06.644138',14,'cris.vergarac@duocuc.cl'),(7,'RESERVA_CONFIRMADA','La reserva cambió de CREADA a CONFIRMADA','2026-09-21 16:12:36.556562',14,'cris.vergarac@duocuc.cl'),(8,'RESERVA_CREADA','Se creó la reserva para el huésped Prueba Frontend','2026-09-21 16:34:49.736529',15,'cris.vergarac@duocuc.cl'),(9,'RESERVA_CONFIRMADA','La reserva cambió de CREADA a CONFIRMADA','2026-09-21 16:35:25.091263',15,'cris.vergarac@duocuc.cl'),(10,'RESERVA_CREADA','Se creó la reserva para el huésped Prueba Presentacion','2026-09-21 18:46:23.705290',16,'cris.vergarac@duocuc.cl'),(11,'RESERVA_CONFIRMADA','La reserva cambió de CREADA a CONFIRMADA','2026-09-21 18:46:57.377422',16,'cris.vergarac@duocuc.cl'),(12,'RESERVA_CREADA','Se creó la reserva para el huésped prueba','2026-09-21 19:44:26.577206',19,'cris.vergarac@duocuc.cl'),(13,'RESERVA_CANCELADA','La reserva cambió de CREADA a CANCELADA','2026-09-21 19:44:34.947846',19,'cris.vergarac@duocuc.cl'),(14,'RESERVA_CONFIRMADA','La reserva cambió de CREADA a CONFIRMADA','2026-09-21 19:44:54.774487',18,'cris.vergarac@duocuc.cl'),(15,'RESERVA_CREADA','Se creó la reserva para el huésped prueba','2026-09-21 19:45:29.422477',20,'cris.vergarac@duocuc.cl'),(16,'RESERVA_CONFIRMADA','La reserva cambió de CREADA a CONFIRMADA','2026-09-21 19:45:52.676045',20,'cris.vergarac@duocuc.cl'),(17,'CHECKIN_PENDIENTE','La reserva cambió de CONFIRMADA a CHECKIN_PENDIENTE','2026-09-21 19:45:57.457593',20,'cris.vergarac@duocuc.cl'),(18,'CHECKIN','La reserva pasó de CHECKIN_PENDIENTE a EN_ESTADÍA','2026-09-21 19:46:00.491657',20,'cris.vergarac@duocuc.cl'),(19,'CHECKOUT','La reserva pasó de EN_ESTADÍA a CHECKOUT','2026-09-21 19:46:03.092138',20,'cris.vergarac@duocuc.cl'),(20,'RESERVA_CREADA','Se creó la reserva para el huésped Prueba Postman','2026-09-21 19:55:07.054718',21,'cris.vergarac@duocuc.cl'),(21,'RESERVA_CONFIRMADA','La reserva cambió de CREADA a CONFIRMADA','2026-09-21 19:55:56.220387',3,'cris.vergarac@duocuc.cl'),(22,'RESERVA_CREADA','Se creó la reserva para el huésped juanillo','2026-09-21 21:30:37.161094',22,'cris.vergarac@duocuc.cl');
/*!40000 ALTER TABLE `audit_events` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `reservations`
--

DROP TABLE IF EXISTS `reservations`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `reservations` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `check_in` date NOT NULL,
  `check_out` date NOT NULL,
  `guest_name` varchar(255) NOT NULL,
  `status` varchar(255) NOT NULL,
  `unit_id` bigint NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=23 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `reservations`
--

LOCK TABLES `reservations` WRITE;
/*!40000 ALTER TABLE `reservations` DISABLE KEYS */;
INSERT INTO `reservations` VALUES (1,'2026-10-25','2026-10-27','Prueba Final','CONFIRMADA',1),(2,'2026-10-24','2026-10-28','lolaso','CREADA',1),(3,'2026-05-10','2026-05-15','sasa','CONFIRMADA',1),(4,'2027-06-01','2027-06-05','Prueba AWS','CREADA',1),(5,'2027-07-01','2027-07-05','Prueba POST AWS','CREADA',1),(6,'2027-07-01','2027-07-05','Prueba POST AWS','CREADA',1),(7,'2027-07-01','2027-07-05','Prueba POST AWS','CREADA',1),(8,'2027-07-01','2027-07-05','Prueba POST AWS','CREADA',1),(9,'2027-07-01','2027-07-05','Prueba POST AWS','CREADA',1),(10,'2027-07-01','2027-07-05','Prueba POST DIRECTO','CREADA',1),(11,'2027-07-01','2027-07-05','Prueba POST DIRECTO','CREADA',1),(12,'2027-07-01','2027-07-05','Prueba POST DIRECTO','CREADA',1),(13,'2027-07-01','2027-07-05','Prueba POST DIRECTO','CREADA',1),(14,'2027-08-01','2027-08-05','Prueba POST API Gateway','CONFIRMADA',1),(15,'2026-09-01','2026-09-05','Prueba Frontend','CONFIRMADA',1),(16,'2027-09-01','2027-09-05','Prueba Presentacion','CONFIRMADA',1),(17,'2026-02-05','2026-02-07','prueba','CREADA',1),(18,'2026-06-10','2026-06-16','prueba','CONFIRMADA',1),(19,'2026-06-04','2026-06-06','prueba','CANCELADA',1),(20,'2026-07-07','2026-07-10','prueba','CHECKOUT',1),(21,'2027-10-20','2027-10-22','Prueba Postman','CREADA',1),(22,'2026-07-11','2026-10-11','juanillo','CREADA',1);
/*!40000 ALTER TABLE `reservations` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `units`
--

DROP TABLE IF EXISTS `units`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `units` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `available` bit(1) NOT NULL,
  `capacity` int NOT NULL,
  `description` varchar(255) DEFAULT NULL,
  `name` varchar(255) NOT NULL,
  `price` decimal(38,2) NOT NULL,
  `type` varchar(255) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `units`
--

LOCK TABLES `units` WRITE;
/*!40000 ALTER TABLE `units` DISABLE KEYS */;
INSERT INTO `units` VALUES (1,_binary '',4,'CabaÃ±a para cuatro personas','CabaÃ±a Los Andes',65000.00,'CABAÃ‘A');
/*!40000 ALTER TABLE `units` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Dumping routines for database 'andesstay'
--
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2026-10-17 19:27:54
