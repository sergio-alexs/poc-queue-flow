# Read Me First
The following was discovered as part of building this project:

* The original package name 'com.example.demo-colas-azure' is invalid and this project uses 'com.example.demo_colas_azure' instead.

# Getting Started

### Reference Documentation
For further reference, please consider the following sections:

* [Official Apache Maven documentation](https://maven.apache.org/guides/index.html)
* [Spring Boot Maven Plugin Reference Guide](https://docs.spring.io/spring-boot/4.0.9/maven-plugin)
* [Create an OCI image](https://docs.spring.io/spring-boot/4.0.9/maven-plugin/build-image.html)
* [Spring Configuration Processor](https://docs.spring.io/spring-boot/4.0.9/specification/configuration-metadata/annotation-processor.html)
* [Spring Web](https://docs.spring.io/spring-boot/4.0.9/reference/web/servlet.html)
* [Spring Data JDBC](https://docs.spring.io/spring-boot/4.0.9/reference/data/sql.html#data.sql.jdbc)
* [Validation](https://docs.spring.io/spring-boot/4.0.9/reference/io/validation.html)

### Guides
The following guides illustrate how to use some features concretely:

* [Building a RESTful Web Service](https://spring.io/guides/gs/rest-service/)
* [Serving Web Content with Spring MVC](https://spring.io/guides/gs/serving-web-content/)
* [Building REST services with Spring](https://spring.io/guides/tutorials/rest/)
* [Using Spring Data JDBC](https://github.com/spring-projects/spring-data-examples/tree/main/jdbc/basics)
* [Validation](https://spring.io/guides/gs/validating-form-input/)

### Maven Parent overrides

Due to Maven's design, elements are inherited from the parent POM to the project POM.
While most of the inheritance is fine, it also inherits unwanted elements like `<license>` and `<developers>` from the parent.
To prevent this, the project POM contains empty overrides for these elements.
If you manually switch to a different parent and actually want the inheritance, you need to remove those overrides.

