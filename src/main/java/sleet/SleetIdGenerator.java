package sleet;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import sleet.generators.GeneratorSessionException;
import sleet.generators.IdGenerator;
import sleet.generators.fixed.FixedLongIdGenerator;
import sleet.generators.reservedinstance.ZooKeeperReservedInstanceIdGenerator;
import sleet.generators.time.TimeDependentSequenceIdGenerator;
import sleet.generators.time.TimeIdGenerator;
import sleet.id.IdType;
import sleet.id.LongIdType;
import sleet.state.IdState;

public class SleetIdGenerator implements IdGenerator<LongIdType> {
  public static final String WAIT_ON_SEQUENCE_OVERRUN_KEY = "sleet.wait.on.sequence.overrun";

  private final List<IdGenerator<? extends IdType<?,?>>> subgens = new ArrayList<IdGenerator<? extends IdType<?, ?>>>(4);
  
  public SleetIdGenerator() {
  }

  @Override
  public void beginIdSession(Properties config) throws SleetException {
    if (!this.subgens.isEmpty()) {
      throw new GeneratorSessionException("Session was already started.  Stop session by calling endIdSession() then start session by calling beginIdSession()");
    }
    TimeIdGenerator timeGen = new TimeIdGenerator();
    timeGen.beginIdSession(config);
    this.subgens.add(timeGen);

    FixedLongIdGenerator fixedGen = new FixedLongIdGenerator();
    fixedGen.beginIdSession(config);
    this.subgens.add(fixedGen);
    
    ZooKeeperReservedInstanceIdGenerator instanceGen = new ZooKeeperReservedInstanceIdGenerator();
    instanceGen.beginIdSession(config);
    this.subgens.add(instanceGen);
    
    TimeDependentSequenceIdGenerator seqGen = new TimeDependentSequenceIdGenerator();
    seqGen.beginIdSession(config);
    this.subgens.add(seqGen);
  }

  @Override
  public void checkSessionValidity() throws SleetException {
    validateSessionStarted();
    for (IdGenerator<?> subgen : this.subgens) {
      subgen.checkSessionValidity();
    }
  }

  private void validateSessionStarted() throws GeneratorSessionException {
    if (this.subgens.isEmpty()) {
      throw new GeneratorSessionException("Session was not started.  Start session by calling beginIdSession()");
    }
  }

  @Override
  public void endIdSession() throws SleetException {
    validateSessionStarted();
    for (IdGenerator<?> subgen : this.subgens) {
      subgen.endIdSession();
    }
    this.subgens.clear();
  }

  public LongIdType getId() throws SleetException {
    validateSessionStarted();
    return getId(null);
  }
  
  @Override
  public LongIdType getId(List<IdState<?, ?>> states) throws SleetException {
    // TODO Auto-generated method stub
    return null;
  }

}
