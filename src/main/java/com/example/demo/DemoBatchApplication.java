package com.example.demo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;

import javax.sql.DataSource;
import java.io.File;
import java.util.Collections;
import java.util.Map;

@EnableBatchProcessing
@SpringBootApplication
public class DemoBatchApplication {

	@Configuration
	public static class Step01Configuration {
		@Autowired
		private DataSource dataSource;

		@Value("${input}")
		Resource input;

		@Bean
		FlatFileItemReader<Person> fileReader() {
			return new FlatFileItemReaderBuilder<Person>()
					.resource(input)
					.name("file-reader")
					.targetType(Person.class)
					.delimited().delimiter(",")
					.names(new String[]{"fileName", "age", "email"})
					.build();
		}

		@Bean
		JdbcBatchItemWriter<Person> jdbcWriter() {
			return new JdbcBatchItemWriterBuilder<Person>()
					.dataSource(dataSource)
					.sql("insert into PEOPLE(AGE, FIRST_NAME, EMAIL) values (:age, :firstName, :email)")
					.beanMapped()
					.build();
		}
	}

	@Configuration
	public static class Step02Configuration {
		@Autowired
		private DataSource dataSource;

		@Value("${output}")
		Resource output;

		@Bean
		ItemReader <Map<Integer, Integer>> jdbcReader() {
			return new JdbcCursorItemReaderBuilder<Map<Integer, Integer>>()
					.dataSource(dataSource)
					.name("jdbc-reader")
					.sql("select COUNT(age) b, age a from PEOPLE group by age")
					.rowMapper((rs, i) -> Collections.singletonMap(
							rs.getInt("a"),
							rs.getInt("b")
					))
					.build();
		}

		@Bean
		ItemWriter<Map<Integer, Integer>> fileWriter () {
			return new FlatFileItemWriterBuilder<Map<Integer, Integer>>()
					.name("file-writer")
					.resource(output)
					.lineAggregator(new DelimitedLineAggregator<Map<Integer, Integer>>(){
						{
							setDelimiter(",");
							setFieldExtractor(integerIntegerMap -> {
								Map.Entry<Integer, Integer> next = integerIntegerMap.entrySet().iterator().next();
								return new Object[]{next.getKey(), next.getValue()};
							});
						}
					})
					.build();
		}

	}

	@Bean
	Job job(JobBuilderFactory jdf,
			StepBuilderFactory sbf,
			Step01Configuration step01Configuration,
			Step02Configuration step02Configuration) {

		Step sl = sbf.get("file-db")
				.<Person, Person>chunk(100)
				.reader(step01Configuration.fileReader())
				.writer(step01Configuration.jdbcWriter())
				.build();

		Step s2 = sbf.get("db-file")
				.<Map<Integer, Integer>, Map<Integer, Integer>>chunk(100)
				.reader(step02Configuration.jdbcReader())
				.writer(step02Configuration.fileWriter())
				.build();

		return jdf.get("etl")
				.incrementer(new RunIdIncrementer())
				.start(sl)
				.next(s2)
				.build();
	}

	public static void main(String[] args) {
		System.setProperty("input", "file://" + new File("/Users/Phayao/Data/in.csv").getAbsolutePath());
		System.setProperty("output", "file://" + new File("/Users/Phayao/Data/out.csv").getAbsolutePath());
		SpringApplication.run(DemoBatchApplication.class, args);

	}

	@Data
	@AllArgsConstructor
	@NoArgsConstructor
	public static class Person {
		private int age;
		private String firstName, email;
	}
}
