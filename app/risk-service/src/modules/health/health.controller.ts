import { Controller, Get } from "@nestjs/common";

@Controller()
export class HealthController {
  @Get("healthz")
  getHealthz() {
    return {
      status: "ok"
    };
  }

  @Get("readyz")
  getReadyz() {
    return {
      status: "ready"
    };
  }
}
