import 'reflect-metadata';
import { NestFactory } from '@nestjs/core';
import { WorkerModule } from './module';

async function bootstrap() {
  await NestFactory.createApplicationContext(WorkerModule);
}
bootstrap();
