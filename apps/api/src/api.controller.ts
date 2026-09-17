import { Body, Controller, Get, Param, Post, Query } from "@nestjs/common";
import { ApiService } from "./api.service";
import { ChangeDto, ConfigDto, PartialDto, SeedDto } from "./api.dto";
import { PipelineAction, SinkName, SinkState } from "./api.types";

@Controller()
export class ApiController {
  constructor(private readonly service: ApiService) {}

  @Get("health") health() {
    return this.service.health();
  }
  @Get("status") status() {
    return this.service.status();
  }
  @Get("replicated") replicated(
    @Query("q") query = "",
    @Query("limit") limit = "50",
  ) {
    return this.service.replicated(query, limit);
  }
  @Get("records") records(
    @Query("q") query = "",
    @Query("limit") limit = "50",
  ) {
    return this.service.records(query, limit);
  }
  @Get("dlq") dlq() {
    return this.service.dlq();
  }

  @Post("pipeline/:action") pipeline(@Param("action") action: PipelineAction) {
    return this.service.pipeline(action);
  }
  @Post("simulation/sink/:sink/:state") sink(
    @Param("sink") sink: SinkName,
    @Param("state") state: SinkState,
  ) {
    return this.service.sink(sink, state);
  }
  @Post("simulation/seed") seed(@Body() body: SeedDto) {
    return this.service.seed(body ?? new SeedDto());
  }
  @Post("simulation/change") change(@Body() body: ChangeDto) {
    return this.service.change(body ?? new ChangeDto());
  }
  @Post("simulation/partial") partial(@Body() body: PartialDto) {
    return this.service.partial(body ?? new PartialDto());
  }
  @Post("dlq/replay") replay() {
    return this.service.replay();
  }
  @Get("config") config() {
    return this.service.config();
  }
  @Post("config") updateConfig(@Body() body: ConfigDto) {
    return this.service.updateConfig(body ?? new ConfigDto());
  }
}
