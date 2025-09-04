using MassTransit;
using MassTransit.RabbitMqTransport.Topology;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using RabbitMQ.Client;
using System.Runtime.Intrinsics.X86;
using System;

namespace MassTransitExchangeTypes;

public record PedidoCriado(string Id, string Pais, bool Sucesso);
public record UsuarioCriado(string Id, string Nome, bool Sucesso);
public record LogEvento(string Texto);

public class UsuarioCriadoConsumer : IConsumer<UsuarioCriado>
{
    public Task Consume(ConsumeContext<UsuarioCriado> context)
    {
        Console.WriteLine($"[UsuarioCriado] {context.Message.Id} / {context.Message.Nome} / ok={context.Message.Sucesso}");
        return Task.CompletedTask;
    }
}


public class PedidoCriadoConsumer : IConsumer<PedidoCriado>
{
    public Task Consume(ConsumeContext<PedidoCriado> context)
    {
        Console.WriteLine($"[PedidoCriado] {context.Message.Id} / {context.Message.Pais} / ok={context.Message.Sucesso}");
        return Task.CompletedTask;
    }
}

public class LogEventoConsumer : IConsumer<LogEvento>
{
    public Task Consume(ConsumeContext<LogEvento> context)
    {
        Console.WriteLine($"[LogEvento] {context.Message.Texto}");
        return Task.CompletedTask;
    }
}

public static class Program
{
    public static async Task Main()
    {
        var host = Host.CreateDefaultBuilder()
            .ConfigureServices(services =>
            {
                services.AddMassTransit(x =>
                {
                    x.AddConsumer<PedidoCriadoConsumer>();
                    x.AddConsumer<LogEventoConsumer>();
                    x.AddConsumer<UsuarioCriadoConsumer>();

                    x.UsingRabbitMq((context, cfg) =>
                    {
                        cfg.Host("localhost", "/", h =>
                        {
                            h.Username("guest");
                            h.Password("guest");
                        });

                        // faz  as configurações das filas, binds e exchanges
                        ConfigureFanoutExchange(context, cfg);
                        ConfigureDirectExchange(context, cfg);
                        ConfigureTopicExchange(context, cfg);

                        //cfg.ConfigureEndpoints(context); MassTransit faz o bind automatico dos consumers com as filas, todas ficam como default fanout.
                    });

                    x.SetKebabCaseEndpointNameFormatter();
                });
            })
            .Build();

        await host.StartAsync();

        // PUBLICAÇÃO DE EXEMPLOS
        var bus = host.Services.GetRequiredService<IBus>();
        await PublicaTodosOsTipos(bus);

        Console.WriteLine("Pressione Enter para encerrar");
        Console.ReadLine();

        await host.StopAsync();
    }

    static async Task PublicaTodosOsTipos(IBus bus)
    {
        // FANOUT
        await bus.Publish(new LogEvento("broadcast para fanout"));
        //-------------------------------------------------------------------

        // DIRECT
        await bus.Publish(new PedidoCriado("7182", "brasil", true), ctx =>
        {
            //routing key:
            ctx.SetRoutingKey("pedido.brasil.sucesso");
        });

        // DIRECT
        await bus.Publish(new PedidoCriado("145236", "estados unidos", true), ctx =>
        {
            //routing key:
            ctx.SetRoutingKey("pedido.eua.falha");
        });

        //-------------------------------------------------------------------
        // TOPIC
        await bus.Publish(new UsuarioCriado("1237", "Cesar Augusto", true), ctx =>
        {
            //routing key:
            ctx.SetRoutingKey("usuario.admin.criado.sucesso");
        });

        // TOPIC
        await bus.Publish(new UsuarioCriado("85697", "Elba Reginao", false), ctx =>
        {
            //routing key:
            ctx.SetRoutingKey("usuario.simples.sucesso");
        });
    }


    /*
       🔹 1. Fanout Exchange
        Como funciona: tudo que for publicado no exchange é copiado para todas as filas ligadas a ele, ignorando chave de roteamento.
        Cenário ideal: broadcast.
        Ex.: serviço de log → toda mensagem é mandada para várias filas(monitoramento, auditoria, backup).
        Exemplo: você publica uma msg "Usuário criado".
        Fila de e-mail recebe.
        Fila de relatórios recebe.
        Fila de analytics recebe.
        → Todos recebem cópia.
    */

    static void ConfigureFanoutExchange(IBusRegistrationContext context, IRabbitMqBusFactoryConfigurator cfg)
    {
        // (opcional) garantir que o publish de LogEvento use fanout
        cfg.Publish<LogEvento>(x => x.ExchangeType = ExchangeType.Fanout);

        // Fila 1 recebe LogEvento
        cfg.ReceiveEndpoint("log-evento-q1", e =>
        {
            e.ConfigureConsumer<LogEventoConsumer>(context);
            // bind automático ao exchange de LogEvento (fanout) é feito pelo MT
        });

        // Fila 2 recebe LogEvento também (broadcast)
        cfg.ReceiveEndpoint("log-evento-q2", e =>
        {
            e.ConfigureConsumer<LogEventoConsumer>(context);
        });
    }



    /*
     🔹 2. Direct Exchange
        Como funciona: roteia mensagens para a fila cuja routing key for exatamente igual à que você enviou na mensagem.
        Cenário ideal: roteamento direto, 1:1.
        Ex.: você tem filas específicas para cada serviço: fila-pagamentos, fila-faturamento.
        Publica com chave pagamentos → vai para fila fila-pagamentos.
     */

    static void ConfigureDirectExchange(IBusRegistrationContext context, IRabbitMqBusFactoryConfigurator cfg)
    {
        // Força o exchange do tipo da mensagem a ser DIRECT
        cfg.Publish<PedidoCriado>(x => x.ExchangeType = ExchangeType.Direct);

        // Fila 1 somente para "pedido.brasil.sucesso"
        cfg.ReceiveEndpoint("pedido-br-sucesso-q", e =>
        {
            e.ConfigureConsumer<PedidoCriadoConsumer>(context);
            e.Bind<PedidoCriado>(bind =>
            {
                bind.ExchangeType = ExchangeType.Direct;
                bind.RoutingKey = "pedido.brasil.sucesso";
            });
        });

        // Fila 2 somente para "pedido.eua.falha"
        cfg.ReceiveEndpoint("pedido-eua-falha-q", e =>
        {
            e.ConfigureConsumer<PedidoCriadoConsumer>(context);
            e.Bind<PedidoCriado>(bind =>
            {
                bind.ExchangeType = ExchangeType.Direct;
                bind.RoutingKey = "pedido.eua.falha";
            });
        });
    }


    /*
     🔹 3. Topic Exchange
        Como funciona: usa padrões na routing key (* para 1 palavra, # para várias).
        Cenário ideal: quando precisa de roteamento flexível.
        Ex.: chave pedido.brasil.sucesso
        pedido.*.sucesso → casa com pedidos de qualquer país que terminaram em sucesso.
        pedido.brasil.# → casa com tudo que é do Brasil.
        Muito usado em sistemas grandes, onde você organiza mensagens em tópicos hierárquicos.
     */
    static void ConfigureTopicExchange(IBusRegistrationContext context, IRabbitMqBusFactoryConfigurator cfg)
    {
        // Força TOPIC para o exchange do tipo UsuarioCriado
        cfg.Publish<UsuarioCriado>(x => x.ExchangeType = ExchangeType.Topic);

        // Fila 1 que recebe "usuario.<qualquer>.sucesso"
        cfg.ReceiveEndpoint("usuario-qualquer-sucesso-q", e =>
        {
            e.ConfigureConsumer<UsuarioCriadoConsumer>(context);
            e.Bind<UsuarioCriado>(bind =>
            {
                bind.ExchangeType = ExchangeType.Topic;
                bind.RoutingKey = "usuario.*.sucesso";
            });
        });

        // Fila 2 que recebe tudo do Admin: "usuario.criado.#"
        cfg.ReceiveEndpoint("usuario-criado-all-q", e =>
        {
            e.ConfigureConsumer<UsuarioCriadoConsumer>(context);
            e.Bind<UsuarioCriado>(bind =>
            {
                bind.ExchangeType = ExchangeType.Topic;
                bind.RoutingKey = "usuario.admin.#";
            });
        });
    }


}


