-- MariaDB dump 10.19  Distrib 10.4.32-MariaDB, for Win64 (AMD64)
--
-- Host: localhost    Database: ecommerce_dw
-- ------------------------------------------------------
-- Server version	10.4.32-MariaDB

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8mb4 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `dim_customer`
--

DROP TABLE IF EXISTS `dim_customer`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dim_customer` (
  `customer_id` int(11) NOT NULL AUTO_INCREMENT,
  `customer_name` varchar(100) DEFAULT NULL,
  `ship_city` varchar(100) DEFAULT NULL,
  `ship_state` varchar(100) DEFAULT NULL,
  `ship_postal_code` varchar(20) DEFAULT NULL,
  `ship_country` varchar(50) DEFAULT NULL,
  PRIMARY KEY (`customer_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `dim_date`
--

DROP TABLE IF EXISTS `dim_date`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dim_date` (
  `date_id` int(11) NOT NULL AUTO_INCREMENT,
  `date` date DEFAULT NULL,
  `day` int(11) DEFAULT NULL,
  `month` int(11) DEFAULT NULL,
  `year` int(11) DEFAULT NULL,
  PRIMARY KEY (`date_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `dim_order_status`
--

DROP TABLE IF EXISTS `dim_order_status`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dim_order_status` (
  `status_id` int(11) NOT NULL AUTO_INCREMENT,
  `status` varchar(50) DEFAULT NULL,
  `courier_status` varchar(50) DEFAULT NULL,
  `b2b_flag` tinyint(1) DEFAULT NULL,
  `promotion_ids` text DEFAULT NULL,
  PRIMARY KEY (`status_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `dim_platform`
--

DROP TABLE IF EXISTS `dim_platform`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dim_platform` (
  `platform_id` int(11) NOT NULL AUTO_INCREMENT,
  `platform_name` varchar(50) DEFAULT NULL,
  `fulfilment_type` varchar(50) DEFAULT NULL,
  `sales_channel` varchar(50) DEFAULT NULL,
  `shipping_level` varchar(50) DEFAULT NULL,
  `fulfilled_by` varchar(50) DEFAULT NULL,
  PRIMARY KEY (`platform_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `dim_product`
--

DROP TABLE IF EXISTS `dim_product`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dim_product` (
  `product_id` int(11) NOT NULL AUTO_INCREMENT,
  `sku_code` varchar(50) DEFAULT NULL,
  `design_no` varchar(50) DEFAULT NULL,
  `style` varchar(50) DEFAULT NULL,
  `category` varchar(100) DEFAULT NULL,
  `size` varchar(10) DEFAULT NULL,
  `color` varchar(30) DEFAULT NULL,
  `asin` varchar(20) DEFAULT NULL,
  PRIMARY KEY (`product_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `fact_international_sales`
--

DROP TABLE IF EXISTS `fact_international_sales`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `fact_international_sales` (
  `intl_sale_id` int(11) NOT NULL AUTO_INCREMENT,
  `date_id` int(11) DEFAULT NULL,
  `product_id` int(11) DEFAULT NULL,
  `customer_id` int(11) DEFAULT NULL,
  `pcs` int(11) DEFAULT NULL,
  `rate` decimal(10,2) DEFAULT NULL,
  `gross_amt` decimal(10,2) DEFAULT NULL,
  PRIMARY KEY (`intl_sale_id`),
  KEY `date_id` (`date_id`),
  KEY `product_id` (`product_id`),
  KEY `customer_id` (`customer_id`),
  CONSTRAINT `fact_international_sales_ibfk_1` FOREIGN KEY (`date_id`) REFERENCES `dim_date` (`date_id`),
  CONSTRAINT `fact_international_sales_ibfk_2` FOREIGN KEY (`product_id`) REFERENCES `dim_product` (`product_id`),
  CONSTRAINT `fact_international_sales_ibfk_3` FOREIGN KEY (`customer_id`) REFERENCES `dim_customer` (`customer_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `fact_inventory`
--

DROP TABLE IF EXISTS `fact_inventory`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `fact_inventory` (
  `inventory_id` int(11) NOT NULL AUTO_INCREMENT,
  `product_id` int(11) DEFAULT NULL,
  `stock_level` int(11) DEFAULT NULL,
  `last_updated_date` date DEFAULT NULL,
  PRIMARY KEY (`inventory_id`),
  KEY `product_id` (`product_id`),
  CONSTRAINT `fact_inventory_ibfk_1` FOREIGN KEY (`product_id`) REFERENCES `dim_product` (`product_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `fact_sales`
--

DROP TABLE IF EXISTS `fact_sales`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `fact_sales` (
  `sale_id` int(11) NOT NULL AUTO_INCREMENT,
  `date_id` int(11) DEFAULT NULL,
  `product_id` int(11) DEFAULT NULL,
  `customer_id` int(11) DEFAULT NULL,
  `platform_id` int(11) DEFAULT NULL,
  `status_id` int(11) DEFAULT NULL,
  `quantity` int(11) DEFAULT NULL,
  `amount` decimal(10,2) DEFAULT NULL,
  `currency` varchar(10) DEFAULT NULL,
  PRIMARY KEY (`sale_id`),
  KEY `date_id` (`date_id`),
  KEY `product_id` (`product_id`),
  KEY `customer_id` (`customer_id`),
  KEY `platform_id` (`platform_id`),
  KEY `status_id` (`status_id`),
  CONSTRAINT `fact_sales_ibfk_1` FOREIGN KEY (`date_id`) REFERENCES `dim_date` (`date_id`),
  CONSTRAINT `fact_sales_ibfk_2` FOREIGN KEY (`product_id`) REFERENCES `dim_product` (`product_id`),
  CONSTRAINT `fact_sales_ibfk_3` FOREIGN KEY (`customer_id`) REFERENCES `dim_customer` (`customer_id`),
  CONSTRAINT `fact_sales_ibfk_4` FOREIGN KEY (`platform_id`) REFERENCES `dim_platform` (`platform_id`),
  CONSTRAINT `fact_sales_ibfk_5` FOREIGN KEY (`status_id`) REFERENCES `dim_order_status` (`status_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2025-07-31 20:20:48
