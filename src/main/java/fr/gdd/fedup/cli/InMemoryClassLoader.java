package fr.gdd.fedup.cli;

class InMemoryClassLoader extends ClassLoader {
    private final String className;
    private final InMemoryLambdaJavaFileObject javaFileObject;

    public InMemoryClassLoader(String className, InMemoryLambdaJavaFileObject javaFileObject) {
        this.className = className;
        this.javaFileObject = javaFileObject;
    }

    @Override
    protected Class<?> findClass(String name) throws ClassNotFoundException {
        if (name.equals(className)) {
            byte[] byteCode = javaFileObject.getByteCode();
            return defineClass(name, byteCode, 0, byteCode.length);
        }
        return super.findClass(name);
    }

}