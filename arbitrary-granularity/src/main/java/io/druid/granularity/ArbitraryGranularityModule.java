// Copyright 2016 Zenysis Inc. All Rights Reserved.
// Author: stephen@zenysis.com (Stephen Ball)

package io.druid.granularity;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.inject.Binder;
import io.druid.initialization.DruidModule;

import java.util.Arrays;
import java.util.List;

public class ArbitraryGranularityModule implements DruidModule
{
  @Override
  public List<? extends Module> getJacksonModules()
  {
    return Arrays.asList(
        new SimpleModule("ArbitraryGranularityModule")
            .registerSubtypes(new NamedType(ArbitraryGranularity.class, "arbitrary"))
    );
  }

  @Override
  public void configure(Binder binder)
  {

  }
}
