import com.espertech.esper.common.client.*;
import com.espertech.esper.common.client.configuration.Configuration;
import com.espertech.esper.compiler.client.*;
import com.espertech.esper.runtime.client.*;

public class Test {

    public static void main(String[] args)
    {
        Configuration configuration = new Configuration();

        configuration.getCommon().addEventType(PersonEvent.class);

        //COMPILING
        //compiler verifies that PersonEvent exists and that the name and age properties are available
        //The compiler generates byte code for extracting property values and producing output events
        //The compiler builds internal data structures for later use by filter indexes to ensure that when a PersonEvent comes in it will be processed fast.
        CompilerArguments compilerArgs = new CompilerArguments(configuration);

        EPCompiler compiler = EPCompilerProvider.getCompiler();
        EPCompiled epCompiled;
        try {
            epCompiled = compiler.compile("@name('my-statement') select name, age from PersonEvent", compilerArgs);
        }
        catch (EPCompileException ex) {
            // handle exception here
            throw new RuntimeException(ex);
        }

        //RUNTIME
        //runtime adds entries to filter indexes -> fast

        EPRuntime runtime = EPRuntimeProvider.getDefaultRuntime(configuration);
        EPDeployment deployment;
        try {
            deployment = runtime.getDeploymentService().deploy(epCompiled);
        }
        catch (EPDeployException ex) {
            // handle exception here
            throw new RuntimeException(ex);
        }

        //create "method" for when my-statement is gotten
        EPStatement statement = runtime.getDeploymentService().getStatement(deployment.getDeploymentId(), "my-statement");
        //add listener(callback) to the statement
        statement.addListener( (newData, oldData, statement2, runtime2) -> {
            String name = (String) newData[0].get("name");
            int age = (int) newData[0].get("age");
            System.out.println(String.format("Name: %s, Age: %d", name, age));
        });

        runtime.getEventService().sendEventBean(new PersonEvent("Peter", 10), "PersonEvent");
    }


}
