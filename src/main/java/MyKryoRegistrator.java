import org.apache.spark.serializer.KryoRegistrator;
import java.io.Serializable;

import com.esotericsoftware.kryo.Kryo;
public class MyKryoRegistrator implements KryoRegistrator, Serializable {
  @Override
  public void registerClasses(Kryo kryo) {
      // Product POJO associated to a product Row from the DataFrame      
      kryo.register(BidAttributes.class);
      kryo.register(CommonRow.class);
      kryo.register(ModifiedRow.class);
      kryo.register(ExtraAttributes.class);
  }
}
