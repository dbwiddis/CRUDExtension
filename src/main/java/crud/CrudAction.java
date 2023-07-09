package crud;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;

import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.OpenSearchException;
import org.opensearch.client.opensearch._types.Result;
import org.opensearch.client.opensearch.core.DeleteRequest;
import org.opensearch.client.opensearch.core.DeleteResponse;
import org.opensearch.client.opensearch.core.GetRequest;
import org.opensearch.client.opensearch.core.GetResponse;
import org.opensearch.client.opensearch.core.IndexRequest;
import org.opensearch.client.opensearch.core.IndexResponse;
import org.opensearch.client.opensearch.core.UpdateRequest;
import org.opensearch.client.opensearch.core.UpdateResponse;
import org.opensearch.client.opensearch.indices.CreateIndexRequest;
import org.opensearch.client.opensearch.indices.ExistsRequest;
import org.opensearch.client.transport.endpoints.BooleanResponse;
import org.opensearch.rest.NamedRoute;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestRequest.Method;
import org.opensearch.rest.RestResponse;
import org.opensearch.rest.RestStatus;
import org.opensearch.sdk.ExtensionsRunner;
import org.opensearch.sdk.rest.BaseExtensionRestHandler;

public class CrudAction extends BaseExtensionRestHandler {

    private OpenSearchClient client;

    public CrudAction(ExtensionsRunner extensionsRunner) {
        this.client = extensionsRunner.getSdkClient().initializeJavaClient();
    }

    @Override
    public List<NamedRoute> routes() {
        return List.of(
            new NamedRoute.Builder().method(Method.PUT)
                .path("/sample")
                .uniqueName("crud_extension:sample/create")
                .handler(createHandler)
                .build(),
            new NamedRoute.Builder().method(Method.GET)
                .path("/sample/{id}")
                .uniqueName("crud_extension:sample/get")
                .handler(readHandler)
                .build(),
            new NamedRoute.Builder().method(Method.POST)
                .path("/sample/{id}")
                .uniqueName("crud_extension:sample/post")
                .handler(updateHandler)
                .build(),
            new NamedRoute.Builder().method(Method.DELETE)
                .path("/sample/{id}")
                .uniqueName("crud_extension:sample/delete")
                .handler(deleteHandler)
                .build()
        );
    }

    Function<RestRequest, RestResponse> createHandler = (request) -> {
        IndexResponse response;
        try {
            // Create index if it doesn't exist
            BooleanResponse exists = client.indices().exists(new ExistsRequest.Builder().index("crudsample").build());
            if (!exists.value()) {
                client.indices().create(new CreateIndexRequest.Builder().index("crudsample").build());
            }
            // Now add our document
            CrudData crudData = new CrudData();
            crudData.setValue("value");
            response = client.index(new IndexRequest.Builder<CrudData>().index("crudsample").document(crudData).build());
        } catch (OpenSearchException | IOException e) {
            return exceptionalRequest(request, e);
        }
        if (response.result() == Result.Created) {
            return createJsonResponse(request, RestStatus.OK, "_id", response.id());
        }
        return createJsonResponse(request, RestStatus.INTERNAL_SERVER_ERROR, "failed", response.result().toString());
    };

    Function<RestRequest, RestResponse> readHandler = (request) -> {
        GetResponse<CrudData> response;
        // Parse ID from request
        String id = request.param("id");
        try {
            response = client.get(new GetRequest.Builder().index("crudsample").id(id).build(), CrudData.class);
        } catch (OpenSearchException | IOException e) {
            return exceptionalRequest(request, e);
        }
        if (response.found()) {
            return createJsonResponse(request, RestStatus.OK, "value", response.source().getValue());
        }
        return createJsonResponse(request, RestStatus.NOT_FOUND, "error", "not_found");
    };

    Function<RestRequest, RestResponse> updateHandler = (request) -> {
        UpdateResponse<CrudData> response;
        // Parse ID from request
        String id = request.param("id");
        // Now create the new document to update with
        CrudData crudData = new CrudData();
        crudData.setValue("new value");
        try {
            response = client.update(
                new UpdateRequest.Builder<CrudData, CrudData>().index("crudsample").id(id).doc(crudData).build(),
                CrudData.class
            );
        } catch (OpenSearchException | IOException e) {
            return exceptionalRequest(request, e);
        }
        if (response.result() == Result.Updated) {
            return createEmptyJsonResponse(request, RestStatus.OK);
        }
        return createJsonResponse(request, RestStatus.INTERNAL_SERVER_ERROR, "failed", response.result().toString());
    };

    Function<RestRequest, RestResponse> deleteHandler = (request) -> {
        DeleteResponse response;
        // Parse ID from request
        String id = request.param("id");
        try {
            response = client.delete(new DeleteRequest.Builder().index("crudsample").id(id).build());
        } catch (OpenSearchException | IOException e) {
            return exceptionalRequest(request, e);
        }
        if (response.result() == Result.Deleted) {
            return createEmptyJsonResponse(request, RestStatus.OK);
        }
        return createJsonResponse(request, RestStatus.INTERNAL_SERVER_ERROR, "failed", response.result().toString());
    };

    public static class CrudData {
        private String value;

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }
    }
}
