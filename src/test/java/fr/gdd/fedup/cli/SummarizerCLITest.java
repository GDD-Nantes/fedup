package fr.gdd.fedup.cli;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SummarizerCLITest {

    @Test
    public void does_not_throw_on_remote_URI() throws IOException, URISyntaxException {
        URI uri = SummarizerCLI.getRemoteURIOrThrow("http://localhost:55555/sparql");
        assertNotNull(uri);
        URI uri2 = SummarizerCLI.getRemoteURIOrThrow("https://some/remote/uri");
        assertNotNull(uri2);
    }

    @Test
    public void throws_on_local_file() {
        // The IOException shows that it tried to open a file
        assertThrows(IOException.class, () -> SummarizerCLI.getRemoteURIOrThrow("./DOES/NOT/EXIST"));
        assertThrows(IOException.class, () -> SummarizerCLI.getRemoteURIOrThrow("/DOES/NOT/EXIST"));
        assertThrows(IOException.class, () -> SummarizerCLI.getRemoteURIOrThrow("file:///DOES/NOT/EXIST"));
    }

}