import { Type } from "class-transformer";
import {
  IsBoolean,
  IsIn,
  IsInt,
  IsOptional,
  IsUUID,
  Max,
  Min,
} from "class-validator";
import { SinkName } from "@kill-it-twice/contracts";

export class SeedDto {
  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1)
  @Max(100000)
  count?: number;

  @IsOptional()
  @IsBoolean()
  reset?: boolean;
}

export class ChangeDto {
  @IsOptional()
  @IsUUID()
  id?: string;

  @IsOptional()
  @IsBoolean()
  invalid?: boolean;

  @IsOptional()
  @IsIn(["search", "events", "both"])
  target?: SinkName | "both";
}

export class PartialDto {
  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1)
  @Max(500)
  count?: number;
}

export class ConfigDto {
  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1)
  @Max(1000)
  batchSize?: number;
}
