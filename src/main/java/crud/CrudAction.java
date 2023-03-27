package crud;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.OpenSearchException;
import org.opensearch.client.opensearch.core.IndexRequest;
import org.opensearch.client.opensearch.core.IndexResponse;
import org.opensearch.client.opensearch.indices.CreateIndexRequest;
import org.opensearch.client.opensearch.indices.ExistsRequest;
import org.opensearch.client.transport.endpoints.BooleanResponse;
import org.opensearch.extensions.rest.ExtensionRestRequest;
import org.opensearch.extensions.rest.ExtensionRestResponse;
import org.opensearch.rest.RestRequest.Method;
import org.opensearch.rest.RestStatus;
import org.opensearch.sdk.BaseExtensionRestHandler;
import org.opensearch.sdk.ExtensionsRunner;
import org.opensearch.sdk.RouteHandler;

public class CrudAction extends BaseExtensionRestHandler {

    private OpenSearchClient client;

    public CrudAction(ExtensionsRunner extensionsRunner) {
        this.client = extensionsRunner.getSdkClient().initializeJavaClient();
    }

    @Override
    protected List<RouteHandler> routeHandlers() {
        return List.of(
            new RouteHandler(Method.PUT, "/sample", createHandler),
            new RouteHandler(Method.GET, "/sample/{id}", readHandler),
            new RouteHandler(Method.POST, "/sample/{id}", updateHandler),
            new RouteHandler(Method.DELETE, "/sample/{id}", deleteHandler)
        );
    }

    Function<ExtensionRestRequest, ExtensionRestResponse> createHandler = (request) -> {
        IndexResponse response;
        try {
            BooleanResponse exists = client.indices().exists(new ExistsRequest.Builder().index("crudsample").build());
            if (!exists.value()) {
                client.indices().create(new CreateIndexRequest.Builder().index("crudsample").build());
            }
            Document doc = new Document();
            doc.add(new StringField("field", "value", Field.Store.YES));
            response = client.index(new IndexRequest.Builder<Document>().index("crudsample").document(doc).build());
        } catch (OpenSearchException | IOException e) {
            return exceptionalRequest(request, e);
        }
        return createJsonResponse(request, RestStatus.OK, "_id", response.id());
    };

    Function<ExtensionRestRequest, ExtensionRestResponse> readHandler = (request) -> {
        return new ExtensionRestResponse(request, RestStatus.OK, "To be implemented");
    };

    Function<ExtensionRestRequest, ExtensionRestResponse> updateHandler = (request) -> {
        return new ExtensionRestResponse(request, RestStatus.OK, "To be implemented");
    };

    Function<ExtensionRestRequest, ExtensionRestResponse> deleteHandler = (request) -> {
        return new ExtensionRestResponse(request, RestStatus.OK, "To be implemented");
    };
}
