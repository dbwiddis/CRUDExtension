package crud;

import java.io.IOException;

import org.opensearch.sdk.BaseExtension;
import org.opensearch.sdk.ExtensionSettings;
import org.opensearch.sdk.ExtensionsRunner;

public class CRUDExtension extends BaseExtension {

    protected CRUDExtension() {
        // Optionally pass a String path to a YAML file with these settings
        super(new ExtensionSettings("crud-extension", "127.0.0.1", "4532", "127.0.0.1", "9200"));
    }

    public static void main(String[] args) throws IOException {
        ExtensionsRunner.run(new CRUDExtension());
    }
}
