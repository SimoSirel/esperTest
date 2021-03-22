import com.espertech.esper.common.client.EPCompiled;
import com.espertech.esper.common.client.configuration.Configuration;
import com.espertech.esper.compiler.client.CompilerArguments;
import com.espertech.esper.compiler.client.EPCompileException;
import com.espertech.esper.compiler.client.EPCompiler;
import com.espertech.esper.compiler.client.EPCompilerProvider;
import com.espertech.esper.runtime.client.*;

import java.util.Arrays;
import java.util.Random;

public class Benchmark {

    public static void main(String[] args) {
        String[] querylist = new String[]{
            //"select * from TemperatureObs",
            //"select tv from TemperatureObs where tv > 50",
            //"select avg(tv) from TemperatureObs",
            //"insert into HighT select * from TemperatureObs where tv > 50",
            //"select avg(hv) from HumidityObs#time(3 seconds)",
            //"create window HWindow#time(3) insert into HWindow select * from HumidityObs",
            //"select avg(hv) from HWindow",
            //"select * from pattern [x=TemperatureObs]",
            //"select * from pattern [every x=TemperatureObs]",
            //"select * from pattern [x=TemperatureObs -> y=HumidityObs]",
            //"select * from pattern [x=TemperatureObs and y=HumidityObs]",
            //"select * from pattern [x=TemperatureObs or y=SensorFailure]",
            //"select * from pattern [(every x=TemperatureObs) timer:within(4)]",
            //"select * from pattern [(every x=TemperatureObs) and not SensorFailure]",
            //"create context PartitionedById partition by sid from TemperatureObs context PartitionedById select sid, max(tv) from TemperatureObs"


            /*
            "insert into HorSF\n" +
                "select * \n" +
                "from pattern [every (x=HumidityObs or y=SensorFailure)];\n" +
                "insert into HpT\n" +
                "select * \n" +
                "from pattern [every (x=HorSF)-> y=TemperatureObs];\n" +
                "create window HpT_windowed#time(3 seconds) as HpT;\n" +
                "insert into HpT_windowed\n" +
                "select * \n" +
                "from HpT;\n" +
                "@name('my-statement')\n" +
                "select x.x.sid, avg(x.x.hv)\n" +
                "from HpT_windowed\n" +
                "where x.x.tv <= y.tv - 20;"
             */
            /*
            "@name('my-statement') "+
            "select x.sid, avg(x.hv)\n" +
                "\n" +
                "from pattern [every (x=HumidityObs or y=SensorFailure) -> z=TemperatureObs]#time(3 seconds)\n" +
                "\n" +
                "where x.tv <= z.tv - 20;"
             */

            /*
            "create context PartitionById\n" +
                "partition by sid from TemperatureObs, sid from SensorFailure;\n" +
                "context PartitionById\n" +
                "insert into HorSF\n" +
                "select * \n" +
                "from pattern [every (x=HumidityObs or y=SensorFailure)];\n" +
                "create context PartitionById1\n" +
                "partition by x.sid from HorSF, sid from TemperatureObs;\n" +
                "context PartitionById1\n" +
                "insert into HpT\n" +
                "select * \n" +
                "from pattern [every (x=HorSF)-> y=TemperatureObs];\n" +
                "create context PartitionById2\n" +
                "partition by x.x.sid from HpT;\n" +
                "context PartitionById2\n" +
                "create window HpT_windowed#time(3 seconds) as HpT;\n" +
                "context PartitionById2\n" +
                "insert into HpT_windowed\n" +
                "select * \n" +
                "from HpT;\n" +
                "@name('my-statement')\n" +
                "context PartitionById2\n" +
                "select x.x.sid, avg(x.x.hv)\n" +
                "from HpT_windowed\n" +
                "where x.x.tv <= y.tv - 20;"
             */

            "create context PartitionById\n" +
                "partition by sid from TemperatureObs, sid from SensorFailure;\n" +
                "@name('my-statement')\n" +
                "select x.sid, avg(x.hv)\n" +
                "from pattern [every (x=HumidityObs or y=SensorFailure) -> z=TemperatureObs]#time(3 seconds)\n" +
                "where x.tv <= z.tv - 20;"





        };


        Configuration configuration = new Configuration();
        configuration.getCommon().addEventType(PersonEvent.class);
        configuration.getCommon().addEventType(TemperatureObs.class);
        configuration.getCommon().addEventType(SensorFailure.class);
        configuration.getCommon().addEventType(HumidityObs.class);

        CompilerArguments compilerArgs = new CompilerArguments(configuration);

        EPCompiler compiler = EPCompilerProvider.getCompiler();
        EPCompiled epCompiled;
        try {
            epCompiled = compiler.compile(querylist[0], compilerArgs);
            //epCompiled = compiler.compile("@name('my-statement') "+querylist[0], compilerArgs);
            //700
            //800
            //1150
            //epCompiled = compiler.compile("insert into randomName "+querylist[0]+"; @name('my-statement') select * from randomName", compilerArgs);
            //1108
            //1111
            //1023

        } catch (EPCompileException ex) {
            // handle exception here
            throw new RuntimeException(ex);
        }

        EPRuntime runtime = EPRuntimeProvider.getDefaultRuntime(configuration);
        runtime.getEventService().clockExternal();
        runtime.getEventService().advanceTime(0);
        EPDeployment deployment;
        try {
            deployment = runtime.getDeploymentService().deploy(epCompiled);
        } catch (EPDeployException ex) {
            throw new RuntimeException(ex);
        }

        EPStatement statement = runtime.getDeploymentService().getStatement(deployment.getDeploymentId(), "my-statement");
        statement.addListener((newData, oldData, statement2, runtime2) -> {
            //String name = (String) newData[0].get("name");
            //int age = (int) newData[0].get("avg(age)");
            Arrays.stream(newData).forEachOrdered(n->{
                //System.out.println(n.getUnderlying());
            });
            //System.out.println(String.format("Age: %d", age));
        });

        Random random = new Random();

        long startTime = System.currentTimeMillis();
        for (int i = 0; i < 1e6; i++) {
            TemperatureObs event = new TemperatureObs(10, random.nextInt());
            String className = "TemperatureObs";
            HumidityObs event2 = new HumidityObs(10, random.nextInt(), random.nextInt());
            String className2 = "HumidityObs";
            SensorFailure event3 = new SensorFailure("xxx", 3, 4);
            String className3 = "SensorFailure";
            runtime.getEventService().sendEventBean(event, className);
            runtime.getEventService().sendEventBean(event2, className2);
            runtime.getEventService().sendEventBean(event3, className3);
            runtime.getEventService().advanceTime(i*1000);
        }
        System.out.println(System.currentTimeMillis() - startTime);

    }
}