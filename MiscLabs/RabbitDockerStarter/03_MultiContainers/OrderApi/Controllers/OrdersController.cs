using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;

namespace OrderApi.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class OrdersController : ControllerBase
    {
        private readonly ILogger<OrdersController> _logger;
        private readonly List<Order> _orders;
        private readonly ConnectionFactory _connectionFactory;

        public OrdersController(ILogger<OrdersController> logger, ConnectionFactory connectionFactory)
        {
            _logger = logger;
            _connectionFactory = connectionFactory;
            _orders = new List<Order>();
        }

        [HttpGet]
        public IEnumerable<Order> Get()
        {
            return _orders;
        }

        [HttpGet("{id:int}")]
        public IActionResult GetById(int id)
        {
            var order = _orders.SingleOrDefault(x => x.Id == id);
            if (order == null)
            {
                return NotFound();
            }
            return Ok(order);
        }

        [HttpPost]
        public IActionResult Post(NewOrderRequest request)
        {
            var newOrder = new Order
            {
                Id = _orders.Max(x => x.Id) + 1,
                Email = request.Email
            };
            _orders.Add(newOrder);
            var newOrderJson = JsonSerializer.Serialize(newOrder);
            _logger.LogInformation($"New order created: {newOrderJson}");

            using var connection = _connectionFactory.CreateConnection();
            using var channel = connection.CreateModel();

            var props = channel.CreateBasicProperties();
            props.AppId = "OrderApi";
            props.Persistent = true;
            props.UserId = _connectionFactory.UserName;
            props.MessageId = Guid.NewGuid().ToString("N");
            props.Timestamp = new AmqpTimestamp(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
            var body = Encoding.UTF8.GetBytes(newOrderJson);
            channel.BasicPublish("emails", "order.created", props, body);

            return CreatedAtAction(nameof(GetById), newOrder.Id, newOrder);
        }
    }

    public class Order
    {
        public int Id { get; set; }
        public string Email { get; set; }
    }

    public class NewOrderRequest
    {
        public string Email { get; set; }
    }
}
