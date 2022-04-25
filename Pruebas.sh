cd /home/dani/Escritorio/SD/P3/P3Despliegue/Pruebas/
mvn clean compile assembly:single
cd /home/dani/Escritorio/SD/P3/P3Despliegue/
java -cp Pruebas/target/Pruebas-1.0-SNAPSHOT-jar-with-dependencies.jar Pruebas

