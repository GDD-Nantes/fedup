package fr.gdd.fedup.cli;

import javax.tools.*;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.List;
import java.util.function.Function;

public class InMemoryLambdaJavaFileObject extends SimpleJavaFileObject {
    private final String sourceCode;
    private byte[] byteCode;

    private final ByteArrayOutputStream outputStream;

    protected InMemoryLambdaJavaFileObject(String className, String body, String type) {
        super(URI.create("string:///" + className.replace('.', '/') + Kind.SOURCE.extension), Kind.SOURCE);

        this.sourceCode = String.format("""
                import java.util.function.Function;
                public class %s {
                public static Function<%s, %s> getFunction() {
                    return %s;
                }
                }""", className, type, type, body);
        this.outputStream = new ByteArrayOutputStream();
    }

    @Override
    public OutputStream openOutputStream() throws IOException {
        return outputStream;
    }

    @Override
    public CharSequence getCharContent(boolean ignoreEncodingErrors) {
        return sourceCode;
    }

    public byte[] getByteCode() {
        return getCompiledBytes();
    }

    public byte[] getCompiledBytes() {
        return outputStream.toByteArray();
    }

    public void setByteCode(byte[] byteCode) {
        this.byteCode = byteCode;
    }


    static <T> Function<T, T> getLambda(String className, String body, String type) {
        InMemoryLambdaJavaFileObject javaFileObject = new InMemoryLambdaJavaFileObject(className, body, type);

        // Prepare the compilation task
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        DiagnosticCollector<JavaFileObject> diagnostics = new DiagnosticCollector<>();
        StandardJavaFileManager stdFileManager = compiler.getStandardFileManager(diagnostics, null, null);
        JavaFileManager fileManager = new ForwardingJavaFileManager<StandardJavaFileManager>(stdFileManager) {
            @Override
            public JavaFileObject getJavaFileForOutput(Location location, String className, JavaFileObject.Kind kind, FileObject sibling) {
                return javaFileObject;
            }
        };

        JavaCompiler.CompilationTask task = compiler.getTask(null, fileManager, diagnostics, null, null, List.of(javaFileObject));
        boolean success = task.call();

        // Check for compilation errors
        if (!success) {
            for (Diagnostic<?> diagnostic : diagnostics.getDiagnostics()) {
                System.out.println(diagnostic);
            }
            return null;
        }

        InMemoryClassLoader classLoader = new InMemoryClassLoader(className, javaFileObject);
        try {
            Class<?> cls = classLoader.loadClass(className);
            return (Function<T, T>) cls.getMethod("getFunction").invoke(null);
        } catch (Exception e) {
            return null;
        }
    }
}
