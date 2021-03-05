package br.com.caelum.camel;

import java.text.SimpleDateFormat;

import org.apache.camel.CamelContext;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.dataformat.xstream.XStreamDataFormat;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.impl.SimpleRegistry;

import com.mysql.cj.jdbc.MysqlConnectionPoolDataSource;
import com.thoughtworks.xstream.XStream;

import br.com.caelum.camel.model.Negociacao;

public class RotaNegiciacoesSaidaDataBase3 {

	public static void main(String[] args) throws Exception {
		
		SimpleRegistry registry = new SimpleRegistry();
		registry.put("mysql", criaDataSource());
		CamelContext context = new DefaultCamelContext(registry);
		
		final XStream xStream = new XStream();
		xStream.alias("negociacao", Negociacao.class);

		
		context.addRoutes(new RouteBuilder() {
			
			@Override
			public void configure() throws Exception {
				from("timer://negociacoes?fixedRate=true&delay=1s&period=360s").
				to("http4://argentumws-spring.herokuapp.com/negociacoes").
				convertBodyTo(String.class).
				unmarshal(new XStreamDataFormat(xStream)).
				split(body()).
				process(exchange -> {
					Negociacao negociacao = exchange.getIn().getBody(Negociacao.class);
					exchange.setProperty("preco", negociacao.getPreco());
					exchange.setProperty("quantidade", negociacao.getQuantidade());
		            String data = new SimpleDateFormat("YYYY-MM-DD hh:mm:ss").format(negociacao.getData().getTime());
		            exchange.setProperty("data", data);
				}).
				setBody(simple("insert into negociacao(preco, quantidade, data) values (${property.preco}, ${property.quantidade}, '${property.data}')")).
				log("${body}").
				delay(1000).
				to("jdbc:mysql");
			}
		});
		
		context.start();
		Thread.sleep(20000);
		context.stop();
	}

	private static MysqlConnectionPoolDataSource criaDataSource() {
		MysqlConnectionPoolDataSource mysqlDs = new MysqlConnectionPoolDataSource();
		mysqlDs.setDatabaseName("camel");
		mysqlDs.setServerName("localhost");
		mysqlDs.setPort(3306);
		mysqlDs.setUser("root");
		mysqlDs.setPassword("Alucard@22");
		return mysqlDs;
	}

}
