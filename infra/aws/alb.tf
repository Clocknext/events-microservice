resource "aws_lb_target_group" "edge" {
  name        = "signal-edge"
  port        = var.edge_port
  protocol    = "HTTP"
  vpc_id      = data.aws_vpc.main.id
  target_type = "instance"

  # `/health`, NOT `/api/v1/health`. Only the signal module is mounted under
  # /api/v1; the health module owns its own `/health` prefix, so /api/v1/health
  # is a 404 and every target would fail its check.
  health_check {
    protocol            = "HTTP"
    path                = "/health"
    matcher             = "200"
    interval            = 30
    timeout             = 5
    healthy_threshold   = 2
    unhealthy_threshold = 3
  }
}

resource "aws_lb_target_group_attachment" "edge" {
  target_group_arn = aws_lb_target_group.edge.arn
  target_id        = aws_instance.edge.id
  port             = var.edge_port
}

resource "aws_lb" "edge" {
  name               = "signal-edge"
  load_balancer_type = "application"
  internal           = false
  security_groups    = [aws_security_group.alb.id]

  # Both AZs, because an ALB requires at least two subnets in distinct AZs even
  # though only one of them holds an instance today.
  subnets = [for s in data.aws_subnet.public : s.id]
}

resource "aws_lb_listener" "http" {
  load_balancer_arn = aws_lb.edge.arn
  port              = 80
  protocol          = "HTTP"

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.edge.arn
  }
}
