package com.example.searchengine.config;

import java.io.IOException;

import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSource;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.multipart.commons.CommonsMultipartResolver;


@Configuration
public class HiveConfig {

	@Value("${hive.connectionURL}")
	private String hiveConnectionURL;

	@Value("${hive.username}")
	private String userName;

	@Value("${hive.password}")
	private String password;

	@Value("${upload.path}")
	private static String uploadPath;

	public static String getUploadPath() {
		return uploadPath;}


	public DataSource getHiveDataSource() throws IOException {  //Connect to Hive
		BasicDataSource dataSource = new BasicDataSource();
		dataSource.setUrl(this.hiveConnectionURL);
		dataSource.setDriverClassName("org.apache.hive.jdbc.HiveDriver");
		dataSource.setUsername(this.userName);
		dataSource.setPassword(this.password);

		return dataSource;
	}
	
	@Bean(name = "jdbcTemplate")
	public JdbcTemplate getJDBCTemplate() throws IOException {
		return new JdbcTemplate(getHiveDataSource());
	}

	@Bean(name = "multipartResolver")
	public CommonsMultipartResolver multipartResolver() {  //Maximum file size allowed - change to allow bigger files
		CommonsMultipartResolver multipartResolver = new CommonsMultipartResolver();
		multipartResolver.setMaxUploadSize(500000000);
		return multipartResolver;
	}
}

