import pygame
import random

# 初始化pygame
pygame.init()

# 设置窗口大小
WIDTH, HEIGHT = 640, 480
screen = pygame.display.set_mode((WIDTH, HEIGHT))

# 设置颜色
WHITE = (255, 255, 255)
GREEN = (0, 255, 0)
RED = (255, 0, 0)

# 设置蛇的初始位置和长度
snake = [(5, 5), (4, 5), (3, 5)]
snake_dir = (1, 0)

# 设置食物的位置
food = (random.randint(0, (WIDTH // 10) - 1) * 10, random.randint(0, (HEIGHT // 10) - 1) * 10)

# 设置游戏的时钟
clock = pygame.time.Clock()

# 游戏主循环
running = True
while running:
    # 处理事件
    for event in pygame.event.get():
        if event.type == pygame.QUIT:
            running = False
        elif event.type == pygame.KEYDOWN:
            if event.key == pygame.K_UP:
                if snake_dir != (0, 1):
                    snake_dir = (0, -1)
            elif event.key == pygame.K_DOWN:
                if snake_dir != (0, -1):
                    snake_dir = (0, 1)
            elif event.key == pygame.K_LEFT:
                if snake_dir != (1, 0):
                    snake_dir = (-1, 0)
            elif event.key == pygame.K_RIGHT:
                if snake_dir != (-1, 0):
                    snake_dir = (1, 0)

    # 移动蛇
    new_head = (snake[0][0] + snake_dir[0], snake[0][1] + snake_dir[1])
    snake.insert(0, new_head)

    # 检查是否吃到食物
    if new_head == food:
        food = (random.randint(0, (WIDTH // 10) - 1) * 10, random.randint(0, (HEIGHT // 10) - 1) * 10)
    else:
        snake.pop()

    # 检查是否碰到墙或自己
    if new_head[0] < 0 or new_head[0] >= WIDTH or new_head[1] < 0 or new_head[1] >= HEIGHT or new_head in snake[1:]:
        running = False

    # 绘制食物和蛇
    screen.fill(WHITE)
    pygame.draw.rect(screen, RED, (food[0], food[1], 10, 10))
    for segment in snake:
        pygame.draw.rect(screen, GREEN, (segment[0], segment[1], 10, 10))

    # 更新显示
    pygame.display.flip()
    clock.tick(10)

# 退出pygame
pygame.quit()