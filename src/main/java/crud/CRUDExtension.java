package crud;

import java.io.IOException;
import java.util.List;

import org.opensearch.sdk.ActionExtension;
import org.opensearch.sdk.BaseExtension;
import org.opensearch.sdk.ExtensionRestHandler;
import org.opensearch.sdk.ExtensionSettings;
import org.opensearch.sdk.ExtensionsRunner;

public class CRUDExtension extends BaseExtension implements ActionExtension {

    public CRUDExtension() {
        // Optionally pass a String path to a YAML file with these settings
        super(new ExtensionSettings("crud-extension", "127.0.0.1", "4532", "127.0.0.1", "9200"));
    }

    @Override
    public List<ExtensionRestHandler> getExtensionRestHandlers() {
        return List.of(new CrudAction(extensionsRunner()));
    }

    public static void main(String[] args) throws IOException {
        ExtensionsRunner.run(new CRUDExtension());
    }
}
