/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.validation;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.Model;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;
import org.mockito.Mock;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.openMocks;

public class ChatCompletionModelValidatorTests extends ESTestCase {
    @Mock
    private ServiceIntegrationValidator mockServiceIntegrationValidator;
    @Mock
    private InferenceService mockInferenceService;
    @Mock
    private InferenceServiceResults mockInferenceServiceResults;
    @Mock
    private Model mockModel;
    @Mock
    private ActionListener<Model> mockActionListener;

    private ChatCompletionModelValidator underTest;

    @Before
    public void setup() {
        openMocks(this);

        underTest = new ChatCompletionModelValidator(mockServiceIntegrationValidator);
    }

    public void testValidate_ServiceIntegrationValidatorThrowsException() {
        doThrow(ElasticsearchStatusException.class).when(mockServiceIntegrationValidator)
            .validate(eq(mockInferenceService), eq(mockModel), any());

        assertThrows(
            ElasticsearchStatusException.class,
            () -> { underTest.validate(mockInferenceService, mockModel, mockActionListener); }
        );

        verify(mockServiceIntegrationValidator).validate(eq(mockInferenceService), eq(mockModel), any());
        verify(mockActionListener).delegateFailureAndWrap(any());
        verifyNoMoreInteractions(
            mockServiceIntegrationValidator,
            mockInferenceService,
            mockInferenceServiceResults,
            mockModel,
            mockActionListener
        );
    }

    public void testValidate_ChatCompletionDetailsUpdated() {
        when(mockActionListener.delegateFailureAndWrap(any())).thenCallRealMethod();
        when(mockInferenceService.updateModelWithChatCompletionDetails(mockModel)).thenReturn(mockModel);
        doAnswer(ans -> {
            ActionListener<InferenceServiceResults> responseListener = ans.getArgument(2);
            responseListener.onResponse(mockInferenceServiceResults);
            return null;
        }).when(mockServiceIntegrationValidator).validate(eq(mockInferenceService), eq(mockModel), any());

        underTest.validate(mockInferenceService, mockModel, mockActionListener);

        verify(mockServiceIntegrationValidator).validate(eq(mockInferenceService), eq(mockModel), any());
        verify(mockActionListener).delegateFailureAndWrap(any());
        verify(mockActionListener).onResponse(mockModel);
        verify(mockInferenceService).updateModelWithChatCompletionDetails(mockModel);
        verifyNoMoreInteractions(
            mockServiceIntegrationValidator,
            mockInferenceService,
            mockInferenceServiceResults,
            mockModel,
            mockActionListener
        );
    }
}
