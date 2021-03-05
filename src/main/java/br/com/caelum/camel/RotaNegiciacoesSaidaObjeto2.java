package br.com.caelum.camel;

import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.http4.HttpMethods;
import org.apache.camel.dataformat.xstream.XStreamDataFormat;
import org.apache.camel.impl.DefaultCamelContext;

import com.thoughtworks.xstream.XStream;

import br.com.caelum.camel.model.Negociacao;

public class RotaNegiciacoesSaidaObjeto2 {

	public static void main(String[] args) throws Exception {
		
		final XStream xStream = new XStream();
		xStream.alias("negociacao", Negociacao.class);

		CamelContext context = new DefaultCamelContext();
		
		context.addRoutes(new RouteBuilder() {
			
			@Override
			public void configure() throws Exception {
				from("timer://negociacoes?fixedRate=true&delay=1s&period=360s").
				to("http4://argentumws-spring.herokuapp.com/negociacoes").
				convertBodyTo(String.class).
				unmarshal(new XStreamDataFormat(xStream)).
				split(body()).
				log("${body}").
				end();
				//setHeader(Exchange.FILE_NAME, constant("negociacoes.xml")).
				//to("file:saida");
			}
		});
		
		context.start();
		Thread.sleep(20000);
		context.stop();
	}	
}
