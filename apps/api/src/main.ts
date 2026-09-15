import "reflect-metadata";
import { NestFactory } from "@nestjs/core";
import { AppModule } from "./module";

async function bootstrap() {
  const app = await NestFactory.create(AppModule);
  app.enableCors();
  app.setGlobalPrefix("api");
  await app.listen(Number(process.env.API_PORT ?? 3000), "0.0.0.0");
}
bootstrap();
